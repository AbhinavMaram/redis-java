package redis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executes commands against the {@link Store}.
 *
 * Real Redis runs commands on a single thread. We get the same guarantee
 * (every command is atomic, MULTI/EXEC blocks are not interleaved) with one
 * global lock, while still handling each client on its own virtual thread.
 */
public final class CommandProcessor {
    private static final String WRONGTYPE =
            Resp.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    private static final String NOT_INT = Resp.error("ERR value is not an integer or out of range");
    private static final String SYNTAX = Resp.error("ERR syntax error");

    private final Store store;
    private final Map<String, String> config;
    private final ReentrantLock lock = new ReentrantLock();

    // Persistence state
    private final Path rdbFile;
    private final List<long[]> savePoints; // {seconds, changes}: snapshot if >= changes writes in >= seconds
    private final AtomicLong dirty = new AtomicLong(); // writes since the last successful save
    private final AtomicBoolean bgsaveInProgress = new AtomicBoolean();
    private volatile long lastSaveMillis = System.currentTimeMillis();
    private volatile boolean shutdownHandled;
    private volatile Runnable shutdownAction = () -> {};

    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "MSET", "DEL", "INCR", "DECR", "INCRBY", "DECRBY", "APPEND", "EXPIRE", "PEXPIRE",
            "PERSIST", "FLUSHALL", "FLUSHDB", "LPUSH", "RPUSH", "LPOP", "RPOP", "EXPIREAT", "PEXPIREAT");

    public CommandProcessor(Store store, Map<String, String> config) {
        this.store = store;
        this.config = config;
        this.rdbFile = Path.of(config.getOrDefault("dir", "."), config.getOrDefault("dbfilename", "dump.rdb"));
        this.savePoints = parseSavePoints(config.getOrDefault("save", ""));
    }

    /** Parses "3600 1 300 100" into {{3600,1},{300,100}}. An empty string disables auto-save. */
    static List<long[]> parseSavePoints(String spec) {
        String[] parts = spec.trim().isEmpty() ? new String[0] : spec.trim().split("\\s+");
        if (parts.length % 2 != 0) throw new IllegalArgumentException("save needs <seconds> <changes> pairs: " + spec);
        List<long[]> out = new ArrayList<>();
        for (int i = 0; i < parts.length; i += 2) {
            out.add(new long[] {Long.parseLong(parts[i]), Long.parseLong(parts[i + 1])});
        }
        return out;
    }

    public void setShutdownAction(Runnable action) {
        this.shutdownAction = action;
    }

    /** Per-connection state. */
    public static final class Session {
        List<List<String>> queued; // non-null while inside MULTI
        boolean queueError;
        int replicaListeningPort; // from REPLCONF listening-port
        Replication.ReplicaLink replicaLink; // set once this connection has sent PSYNC
    }

    private final Replication repl = new Replication();

    public Replication replication() {
        return repl;
    }

    public String handle(List<String> args, Session session) {
        if (args.isEmpty()) return "";
        String name = args.get(0).toUpperCase(Locale.ROOT);

        if (repl.isReplica() && WRITE_COMMANDS.contains(name)) {
            if (session.queued != null) session.queueError = true;
            return Resp.error("READONLY You can't write against a read only replica.");
        }
        if (name.equals("REPLCONF") && args.size() >= 2) return replconf(args, session);
        if (name.equals("WAIT") && session.queued == null) return waitForReplicas(args);

        if (session.queued != null) {
            switch (name) {
                case "EXEC":
                    return exec(session);
                case "DISCARD":
                    session.queued = null;
                    session.queueError = false;
                    return Resp.OK;
                case "MULTI":
                    return Resp.error("ERR MULTI calls can not be nested");
                default:
                    String arityErr = checkKnown(name, args);
                    if (arityErr != null) {
                        session.queueError = true;
                        return arityErr;
                    }
                    session.queued.add(args);
                    return Resp.simple("QUEUED");
            }
        }

        switch (name) {
            case "MULTI":
                session.queued = new ArrayList<>();
                session.queueError = false;
                return Resp.OK;
            case "EXEC":
                return Resp.error("ERR EXEC without MULTI");
            case "DISCARD":
                return Resp.error("ERR DISCARD without MULTI");
            case "SHUTDOWN":
                // Handled outside the lock: the shutdown action exits the JVM, and exit
                // hooks need the lock to save.
                return shutdown(args);
            default:
                lock.lock();
                try {
                    return execute(name, args);
                } finally {
                    lock.unlock();
                }
        }
    }

    private String exec(Session session) {
        List<List<String>> cmds = session.queued;
        boolean aborted = session.queueError;
        session.queued = null;
        session.queueError = false;
        if (aborted) return Resp.error("EXECABORT Transaction discarded because of previous errors.");

        List<String> replies = new ArrayList<>(cmds.size());
        boolean anyWrite = cmds.stream().anyMatch(c -> WRITE_COMMANDS.contains(c.get(0).toUpperCase(Locale.ROOT)));
        lock.lock();
        try {
            // Wrap the writes in MULTI/EXEC on the stream too, so replicas apply them atomically.
            if (anyWrite) repl.propagate(List.of("MULTI"));
            for (List<String> cmd : cmds) {
                replies.add(execute(cmd.get(0).toUpperCase(Locale.ROOT), cmd));
            }
            if (anyWrite) repl.propagate(List.of("EXEC"));
        } finally {
            lock.unlock();
        }
        return Resp.rawArray(replies);
    }

    /** Validates name + arity without running; used when queueing inside MULTI. */
    private String checkKnown(String name, List<String> args) {
        Integer arity = ARITY.get(name);
        if (arity == null) return unknown(args);
        return arityOk(arity, args.size()) ? null : wrongArgs(name);
    }

    // Redis-style arity: positive = exact count, negative = at least |n|. Includes the command name.
    private static final Map<String, Integer> ARITY = Map.ofEntries(
            Map.entry("PING", -1), Map.entry("ECHO", 2), Map.entry("SET", -3), Map.entry("GET", 2),
            Map.entry("DEL", -2), Map.entry("EXISTS", -2), Map.entry("INCR", 2), Map.entry("DECR", 2),
            Map.entry("INCRBY", 3), Map.entry("DECRBY", 3), Map.entry("APPEND", 3), Map.entry("STRLEN", 2),
            Map.entry("MGET", -2), Map.entry("MSET", -3),
            Map.entry("EXPIRE", 3), Map.entry("PEXPIRE", 3), Map.entry("TTL", 2), Map.entry("PTTL", 2),
            Map.entry("PERSIST", 2), Map.entry("TYPE", 2), Map.entry("KEYS", 2), Map.entry("DBSIZE", 1),
            Map.entry("FLUSHALL", -1), Map.entry("FLUSHDB", -1),
            Map.entry("LPUSH", -3), Map.entry("RPUSH", -3), Map.entry("LPOP", -2), Map.entry("RPOP", -2),
            Map.entry("LLEN", 2), Map.entry("LRANGE", 4),
            Map.entry("CONFIG", -2), Map.entry("INFO", -1), Map.entry("COMMAND", -1),
            Map.entry("SAVE", 1), Map.entry("BGSAVE", -1), Map.entry("LASTSAVE", 1),
            Map.entry("EXPIREAT", 3), Map.entry("PEXPIREAT", 3), Map.entry("SELECT", 2),
            Map.entry("REPLCONF", -2), Map.entry("PSYNC", 3), Map.entry("WAIT", 3));

    private static boolean arityOk(int arity, int n) {
        return arity >= 0 ? n == arity : n >= -arity;
    }

    /** Runs one command. Caller must hold the lock. */
    private String execute(String name, List<String> a) {
        String reply = run(name, a);
        if (WRITE_COMMANDS.contains(name) && !reply.startsWith("-")) {
            dirty.incrementAndGet();
            if (repl.hasReplicas()) {
                List<String> forReplicas = replicationForm(name, a);
                if (forReplicas != null) repl.propagate(forReplicas);
            }
        }
        return reply;
    }

    /**
     * What to send replicas for a write that just ran, or null to send nothing.
     * Relative expiry (EX, PX, EXPIRE) becomes an absolute time, so a replica
     * that applies the command a little later still expires the key at the
     * same moment as the master.
     */
    private List<String> replicationForm(String name, List<String> a) {
        switch (name) {
            case "SET": {
                String key = a.get(1), value = a.get(2);
                Store.Entry e = store.getEntry(key);
                if (e == null) return List.of("DEL", key); // XX found nothing, or PXAT was already in the past
                // set() stores the argument String itself, so identity tells us if this SET wrote (NX/XX may skip).
                if (e.value != value) return null;
                return e.expiresAt == Store.NO_EXPIRY
                        ? List.of("SET", key, value)
                        : List.of("SET", key, value, "PXAT", Long.toString(e.expiresAt));
            }
            case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT": {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) return List.of("DEL", a.get(1)); // a time in the past deletes the key
                return List.of("PEXPIREAT", a.get(1), Long.toString(e.expiresAt));
            }
            default:
                return a;
        }
    }

    /** INFO [section ...]: no section, "all" or "everything" means every section. */
    private String info(List<String> a) {
        Map<String, String> sections = new java.util.LinkedHashMap<>();
        sections.put("server", "# Server\r\nredis_version:7.0.0-java\r\ntcp_port:"
                + config.getOrDefault("port", "?") + "\r\n");
        sections.put("persistence", "# Persistence\r\n"
                + "rdb_changes_since_last_save:" + dirty.get() + "\r\n"
                + "rdb_bgsave_in_progress:" + (bgsaveInProgress.get() ? 1 : 0) + "\r\n"
                + "rdb_last_save_time:" + lastSaveMillis / 1000 + "\r\n");
        sections.put("replication", repl.info());
        sections.put("keyspace", "# Keyspace\r\ndb0:keys=" + store.size() + "\r\n");

        List<String> wanted = new ArrayList<>();
        for (int i = 1; i < a.size(); i++) wanted.add(a.get(i).toLowerCase(Locale.ROOT));
        boolean all = wanted.isEmpty() || wanted.contains("all") || wanted.contains("everything");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> s : sections.entrySet()) {
            if (all || wanted.contains(s.getKey())) {
                if (!sb.isEmpty()) sb.append("\r\n");
                sb.append(s.getValue());
            }
        }
        return Resp.bulk(sb.toString());
    }

    // ----------------------------------------------------------- replication

    /** Master side of PSYNC: from here on, this connection carries the replication stream. */
    public void psync(java.net.Socket socket, java.io.OutputStream out, Session session) {
        lock.lock();
        try {
            session.replicaLink = repl.register(socket, out, session.replicaListeningPort, store.snapshot());
        } finally {
            lock.unlock();
        }
    }

    public void replicaDisconnected(Session session) {
        if (session.replicaLink != null) repl.remove(session.replicaLink);
    }

    private String replconf(List<String> a, Session session) {
        String sub = a.get(1).toUpperCase(Locale.ROOT);
        switch (sub) {
            case "ACK": {
                // A replica reporting its offset. Never replied to.
                Long off = a.size() >= 3 ? parseLong(a.get(2)) : null;
                if (session.replicaLink != null && off != null) repl.ack(session.replicaLink, off);
                return "";
            }
            case "LISTENING-PORT": {
                Long port = a.size() >= 3 ? parseLong(a.get(2)) : null;
                if (port == null) return SYNTAX;
                session.replicaListeningPort = port.intValue();
                return Resp.OK;
            }
            default:
                return Resp.OK; // capa, ip-address, ...: accepted and ignored
        }
    }

    /** WAIT numreplicas timeout: block until that many replicas have every write so far. */
    private String waitForReplicas(List<String> a) {
        if (a.size() != 3) return wrongArgs("WAIT");
        Long n = parseLong(a.get(1)), timeout = parseLong(a.get(2));
        if (n == null || timeout == null) return NOT_INT;
        if (timeout < 0) return Resp.error("ERR timeout is negative");
        if (repl.isReplica()) return Resp.error("ERR WAIT cannot be used with replica instances.");

        long target;
        lock.lock();
        try {
            target = repl.masterOffset;
            int acked = repl.countAcked(target);
            if (acked >= n || !repl.hasReplicas()) return Resp.integer(acked);
            // Ask every replica for its offset. GETACK itself goes on the stream after target.
            repl.propagate(List.of("REPLCONF", "GETACK", "*"));
        } finally {
            lock.unlock();
        }
        try {
            return Resp.integer(repl.awaitAcks(target, n.intValue(), timeout));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Resp.integer(repl.countAcked(target));
        }
    }

    /** Replica side: replaces the keyspace with the master's snapshot. */
    public int loadFromMaster(byte[] rdb) throws IOException {
        List<Rdb.Item> items = Rdb.read(rdb);
        lock.lock();
        try {
            return store.load(items);
        } finally {
            lock.unlock();
        }
    }

    /** Replica side: applies commands from the master's stream atomically, ignoring replies. */
    public void applyFromMaster(List<List<String>> commands) {
        lock.lock();
        try {
            for (List<String> cmd : commands) {
                if (!cmd.isEmpty()) execute(cmd.get(0).toUpperCase(Locale.ROOT), cmd);
            }
        } finally {
            lock.unlock();
        }
    }

    private String run(String name, List<String> a) {
        Integer arity = ARITY.get(name);
        if (arity == null) return unknown(a);
        if (!arityOk(arity, a.size())) return wrongArgs(name);

        return switch (name) {
            case "SAVE" -> {
                String err = saveLocked();
                yield err == null ? Resp.OK : Resp.error("ERR " + err);
            }
            case "BGSAVE" -> bgsaveLocked()
                    ? Resp.simple("Background saving started")
                    : Resp.error("ERR Background save already in progress");
            case "LASTSAVE" -> Resp.integer(lastSaveMillis / 1000);
            case "EXPIREAT", "PEXPIREAT" -> {
                Long at = parseLong(a.get(2));
                if (at == null) yield NOT_INT;
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.integer(0);
                long ms = name.equals("EXPIREAT") ? at * 1000 : at;
                if (ms <= store.now()) store.remove(a.get(1));
                else e.expiresAt = ms;
                yield Resp.integer(1);
            }
            case "SELECT" -> a.get(1).equals("0") ? Resp.OK : Resp.error("ERR DB index is out of range");
            case "REPLCONF" -> Resp.OK;
            case "PSYNC" -> Resp.error("ERR PSYNC must be sent on its own connection, outside MULTI");
            case "WAIT" -> Resp.integer(repl.countAcked(repl.masterOffset)); // inside MULTI: don't block
            case "PING" -> a.size() == 1 ? Resp.simple("PONG") : Resp.bulk(a.get(1));
            case "ECHO" -> Resp.bulk(a.get(1));
            case "SET" -> set(a);
            case "GET" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.NULL_BULK;
                if (!(e.value instanceof String s)) yield WRONGTYPE;
                yield Resp.bulk(s);
            }
            case "MGET" -> {
                List<String> out = new ArrayList<>();
                for (int i = 1; i < a.size(); i++) {
                    Store.Entry e = store.getEntry(a.get(i));
                    out.add(e != null && e.value instanceof String s ? s : null);
                }
                yield Resp.bulkArray(out);
            }
            case "MSET" -> {
                if (a.size() % 2 != 1) yield wrongArgs(name);
                for (int i = 1; i < a.size(); i += 2) store.put(a.get(i), a.get(i + 1), Store.NO_EXPIRY);
                yield Resp.OK;
            }
            case "DEL" -> {
                int n = 0;
                for (int i = 1; i < a.size(); i++) if (store.remove(a.get(i))) n++;
                yield Resp.integer(n);
            }
            case "EXISTS" -> {
                int n = 0;
                for (int i = 1; i < a.size(); i++) if (store.getEntry(a.get(i)) != null) n++;
                yield Resp.integer(n);
            }
            case "INCR" -> incrBy(a.get(1), "1");
            case "DECR" -> incrBy(a.get(1), "-1");
            case "INCRBY" -> incrBy(a.get(1), a.get(2));
            case "DECRBY" -> a.get(2).startsWith("-") ? incrBy(a.get(1), a.get(2).substring(1)) : incrBy(a.get(1), "-" + a.get(2));
            case "APPEND" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) {
                    store.put(a.get(1), a.get(2), Store.NO_EXPIRY);
                    yield Resp.integer(utf8Len(a.get(2)));
                }
                if (!(e.value instanceof String s)) yield WRONGTYPE;
                e.value = s + a.get(2);
                yield Resp.integer(utf8Len((String) e.value));
            }
            case "STRLEN" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.integer(0);
                if (!(e.value instanceof String s)) yield WRONGTYPE;
                yield Resp.integer(utf8Len(s));
            }
            case "EXPIRE", "PEXPIRE" -> {
                Long amount = parseLong(a.get(2));
                if (amount == null) yield NOT_INT;
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.integer(0);
                long ms = name.equals("EXPIRE") ? amount * 1000 : amount;
                if (ms <= 0) store.remove(a.get(1));
                else e.expiresAt = store.now() + ms;
                yield Resp.integer(1);
            }
            case "TTL", "PTTL" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.integer(-2);
                if (e.expiresAt == Store.NO_EXPIRY) yield Resp.integer(-1);
                long ms = e.expiresAt - store.now();
                // Redis rounds TTL to the nearest second.
                yield Resp.integer(name.equals("TTL") ? (ms + 500) / 1000 : ms);
            }
            case "PERSIST" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null || e.expiresAt == Store.NO_EXPIRY) yield Resp.integer(0);
                e.expiresAt = Store.NO_EXPIRY;
                yield Resp.integer(1);
            }
            case "TYPE" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.simple("none");
                yield Resp.simple(e.value instanceof String ? "string" : "list");
            }
            case "KEYS" -> Resp.bulkArray(store.keys(a.get(1)));
            case "DBSIZE" -> Resp.integer(store.size());
            case "FLUSHALL", "FLUSHDB" -> {
                store.clear();
                yield Resp.OK;
            }
            case "LPUSH", "RPUSH" -> {
                ArrayDeque<String> list = listForWrite(a.get(1));
                if (list == null) yield WRONGTYPE;
                for (int i = 2; i < a.size(); i++) {
                    if (name.equals("LPUSH")) list.addFirst(a.get(i));
                    else list.addLast(a.get(i));
                }
                yield Resp.integer(list.size());
            }
            case "LPOP", "RPOP" -> pop(name, a);
            case "LLEN" -> {
                Store.Entry e = store.getEntry(a.get(1));
                if (e == null) yield Resp.integer(0);
                if (!(e.value instanceof ArrayDeque<?> l)) yield WRONGTYPE;
                yield Resp.integer(l.size());
            }
            case "LRANGE" -> lrange(a);
            case "CONFIG" -> config(a);
            case "INFO" -> info(a);
            case "COMMAND" -> Resp.bulkArray(List.of()); // enough for redis-cli's startup probe
            default -> unknown(a);
        };
    }

    // SET key value [NX | XX] [GET] [EX s | PX ms | EXAT s | PXAT ms | KEEPTTL]
    private String set(List<String> a) {
        String key = a.get(1), value = a.get(2);
        boolean nx = false, xx = false, get = false, keepTtl = false;
        long expiresAt = Store.NO_EXPIRY;
        boolean expirySet = false;

        for (int i = 3; i < a.size(); i++) {
            String opt = a.get(i).toUpperCase(Locale.ROOT);
            switch (opt) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GET" -> get = true;
                case "KEEPTTL" -> keepTtl = true;
                case "EX", "PX", "EXAT", "PXAT" -> {
                    if (expirySet || i + 1 >= a.size()) return SYNTAX;
                    Long n = parseLong(a.get(++i));
                    if (n == null) return NOT_INT;
                    if (n <= 0) return Resp.error("ERR invalid expire time in 'set' command");
                    expiresAt = switch (opt) {
                        case "EX" -> store.now() + n * 1000;
                        case "PX" -> store.now() + n;
                        case "EXAT" -> n * 1000;
                        default -> n;
                    };
                    expirySet = true;
                }
                default -> {
                    return SYNTAX;
                }
            }
        }
        if ((nx && xx) || (keepTtl && expirySet)) return SYNTAX;

        Store.Entry existing = store.getEntry(key);
        if (get && existing != null && !(existing.value instanceof String)) return WRONGTYPE;
        String old = existing != null && existing.value instanceof String s ? s : null;

        boolean doSet = !(nx && existing != null) && !(xx && existing == null);
        if (doSet) {
            if (keepTtl && existing != null) expiresAt = existing.expiresAt;
            store.put(key, value, expiresAt);
        }
        if (get) return Resp.bulk(old);
        return doSet ? Resp.OK : Resp.NULL_BULK;
    }

    private String incrBy(String key, String deltaStr) {
        Long delta = parseLong(deltaStr);
        if (delta == null) return NOT_INT;
        Store.Entry e = store.getEntry(key);
        long current = 0;
        if (e != null) {
            if (!(e.value instanceof String s)) return WRONGTYPE;
            Long parsed = parseLong(s);
            if (parsed == null) return NOT_INT;
            current = parsed;
        }
        long next;
        try {
            next = Math.addExact(current, delta);
        } catch (ArithmeticException ex) {
            return Resp.error("ERR increment or decrement would overflow");
        }
        if (e == null) store.put(key, Long.toString(next), Store.NO_EXPIRY);
        else e.value = Long.toString(next); // INCR keeps the existing TTL
        return Resp.integer(next);
    }

    @SuppressWarnings("unchecked")
    private ArrayDeque<String> listForWrite(String key) {
        Store.Entry e = store.getEntry(key);
        if (e == null) {
            ArrayDeque<String> list = new ArrayDeque<>();
            store.put(key, list, Store.NO_EXPIRY);
            return list;
        }
        return e.value instanceof ArrayDeque<?> l ? (ArrayDeque<String>) l : null;
    }

    @SuppressWarnings("unchecked")
    private String pop(String name, List<String> a) {
        if (a.size() > 3) return SYNTAX;
        Long count = null;
        if (a.size() == 3) {
            count = parseLong(a.get(2));
            if (count == null || count < 0) return Resp.error("ERR value is out of range, must be positive");
        }
        Store.Entry e = store.getEntry(a.get(1));
        if (e == null) return count == null ? Resp.NULL_BULK : Resp.NULL_ARRAY;
        if (!(e.value instanceof ArrayDeque<?> raw)) return WRONGTYPE;
        ArrayDeque<String> list = (ArrayDeque<String>) raw;

        List<String> popped = new ArrayList<>();
        long n = count == null ? 1 : count;
        while (n-- > 0 && !list.isEmpty()) {
            popped.add(name.equals("LPOP") ? list.pollFirst() : list.pollLast());
        }
        if (list.isEmpty()) store.remove(a.get(1)); // empty lists don't exist in Redis
        return count == null ? Resp.bulk(popped.isEmpty() ? null : popped.get(0)) : Resp.bulkArray(popped);
    }

    private String lrange(List<String> a) {
        Long start = parseLong(a.get(2)), stop = parseLong(a.get(3));
        if (start == null || stop == null) return NOT_INT;
        Store.Entry e = store.getEntry(a.get(1));
        if (e == null) return Resp.bulkArray(List.of());
        if (!(e.value instanceof ArrayDeque<?> raw)) return WRONGTYPE;
        List<String> items = new ArrayList<>();
        for (Object o : raw) items.add((String) o);

        int len = items.size();
        long s = start < 0 ? len + start : start;
        long t = stop < 0 ? len + stop : stop;
        s = Math.max(s, 0);
        t = Math.min(t, len - 1);
        if (s > t) return Resp.bulkArray(List.of());
        return Resp.bulkArray(items.subList((int) s, (int) t + 1));
    }

    private String config(List<String> a) {
        String sub = a.get(1).toUpperCase(Locale.ROOT);
        if (!sub.equals("GET") || a.size() != 3) {
            return Resp.error("ERR unknown subcommand or wrong number of arguments for 'CONFIG'");
        }
        List<String> out = new ArrayList<>();
        java.util.regex.Pattern p = Store.globToRegex(a.get(2).toLowerCase(Locale.ROOT));
        for (Map.Entry<String, String> c : config.entrySet()) {
            if (p.matcher(c.getKey()).matches()) {
                out.add(c.getKey());
                out.add(c.getValue());
            }
        }
        return Resp.bulkArray(out);
    }

    private static Long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int utf8Len(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    private static String wrongArgs(String name) {
        return Resp.error("ERR wrong number of arguments for '" + name.toLowerCase(Locale.ROOT) + "' command");
    }

    private static String unknown(List<String> a) {
        return Resp.error("ERR unknown command '" + a.get(0) + "'");
    }

    // ----------------------------------------------------------- persistence

    /** Loads the RDB file if it exists. Called once at startup. Returns keys loaded, or -1 if no file. */
    public int loadFromDisk() throws IOException {
        if (!Files.exists(rdbFile)) return -1;
        List<Rdb.Item> items = Rdb.readFile(rdbFile);
        lock.lock();
        try {
            return store.load(items);
        } finally {
            lock.unlock();
        }
    }

    public Path rdbFile() {
        return rdbFile;
    }

    /** SAVE: write synchronously, blocking every client (like Redis). Returns an error message or null. */
    private String saveLocked() {
        if (bgsaveInProgress.get()) return "Background save already in progress";
        long dirtyAtSnapshot = dirty.get();
        try {
            Rdb.writeFile(rdbFile, store.snapshot());
        } catch (IOException e) {
            System.err.println("SAVE failed: " + e);
            return "failed to save: " + e.getMessage();
        }
        dirty.addAndGet(-dirtyAtSnapshot);
        lastSaveMillis = System.currentTimeMillis();
        return null;
    }

    /**
     * BGSAVE: copy the keyspace while holding the lock (fast), then serialize and
     * write it on another thread so clients keep being served. Real Redis uses
     * fork() and copy-on-write for the same effect.
     */
    private boolean bgsaveLocked() {
        if (!bgsaveInProgress.compareAndSet(false, true)) return false;
        List<Rdb.Item> snapshot = store.snapshot();
        long dirtyAtSnapshot = dirty.get();
        Thread.ofVirtual().name("bgsave").start(() -> {
            try {
                Rdb.writeFile(rdbFile, snapshot);
                dirty.addAndGet(-dirtyAtSnapshot); // writes made during the save still count as unsaved
                lastSaveMillis = System.currentTimeMillis();
            } catch (IOException e) {
                System.err.println("BGSAVE failed: " + e);
            } finally {
                bgsaveInProgress.set(false);
            }
        });
        return true;
    }

    /** Called every second: starts a BGSAVE if any "save <seconds> <changes>" rule is met. */
    public void checkSavePoints() {
        long changes = dirty.get();
        if (changes == 0 || bgsaveInProgress.get()) return;
        long elapsed = (System.currentTimeMillis() - lastSaveMillis) / 1000;
        for (long[] sp : savePoints) {
            if (elapsed >= sp[0] && changes >= sp[1]) {
                lock.lock();
                try {
                    bgsaveLocked();
                } finally {
                    lock.unlock();
                }
                return;
            }
        }
    }

    /** SHUTDOWN [NOSAVE|SAVE]: saves (if save points are set, or SAVE is given) and exits. */
    private String shutdown(List<String> a) {
        boolean noSave = false, forceSave = false;
        for (int i = 1; i < a.size(); i++) {
            switch (a.get(i).toUpperCase(Locale.ROOT)) {
                case "NOSAVE" -> noSave = true;
                case "SAVE" -> forceSave = true;
                default -> {
                    return SYNTAX;
                }
            }
        }
        if (noSave && forceSave) return SYNTAX;
        if (!noSave && (forceSave || !savePoints.isEmpty())) {
            String err = waitAndSave();
            if (err != null) return Resp.error("ERR Errors trying to SHUTDOWN. Check logs. (" + err + ")");
        }
        shutdownHandled = true;
        shutdownAction.run();
        return Resp.OK; // only reached when the shutdown action does not exit (tests)
    }

    /** JVM exit hook (Ctrl+C): save if auto-save is on and there are unsaved changes. */
    public void saveOnExit() {
        if (shutdownHandled || savePoints.isEmpty() || dirty.get() == 0) return;
        String err = waitAndSave();
        System.out.println(err == null ? "Saved " + rdbFile + " before exit" : "Save before exit failed: " + err);
    }

    private String waitAndSave() {
        while (bgsaveInProgress.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "interrupted";
            }
        }
        lock.lock();
        try {
            return saveLocked();
        } finally {
            lock.unlock();
        }
    }

    /** Runs the active-expiry sweep under the global lock. */
    public void sweepExpired() {
        lock.lock();
        try {
            store.sweepExpired();
        } finally {
            lock.unlock();
        }
    }
}
