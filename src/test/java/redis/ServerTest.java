package redis;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** End-to-end tests over a real TCP socket. No framework needed: run with `java`. */
public final class ServerTest {
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        String tmp = java.nio.file.Files.createTempDirectory("redis-test").toString();
        try (RedisServer server = new RedisServer(0, Map.of("dir", tmp, "dbfilename", "dump.rdb", "save", ""))) {
            Thread t = new Thread(server::serve);
            t.setDaemon(true);
            t.start();
            int port = server.port();

            try (Client c = new Client(port)) {
                basics(c);
                strings(c);
                expiry(c);
                lists(c);
                transactions(c, port);
                misc(c, tmp);
            }
            pipelining(port);
            concurrentClients(port);
            inlineCommands(port);
        }
        summary();
    }

    static void summary() {
        System.out.printf("%n%d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    static void basics(Client c) throws IOException {
        check("PING", "PONG", c.cmd("PING"));
        check("PING msg", "hi", c.cmd("PING", "hi"));
        check("ECHO", "hello world", c.cmd("ECHO", "hello world"));
        check("ECHO utf8", "hÃ©llo âœ“", c.cmd("ECHO", "hÃ©llo âœ“"));
        check("unknown", "ERR unknown command 'NOPE'", c.cmd("NOPE"));
        check("arity", "ERR wrong number of arguments for 'get' command", c.cmd("GET"));
        check("case-insensitive", "PONG", c.cmd("ping"));
    }

    static void strings(Client c) throws IOException {
        check("SET", "OK", c.cmd("SET", "k", "v"));
        check("GET", "v", c.cmd("GET", "k"));
        check("GET missing", null, c.cmd("GET", "missing"));
        check("SET NX existing", null, c.cmd("SET", "k", "x", "NX"));
        check("SET XX missing", null, c.cmd("SET", "nope", "x", "XX"));
        check("SET XX existing", "OK", c.cmd("SET", "k", "v2", "XX"));
        check("SET GET", "v2", c.cmd("SET", "k", "v3", "GET"));
        check("EXISTS", 1L, c.cmd("EXISTS", "k", "missing"));
        check("DEL", 1L, c.cmd("DEL", "k", "missing"));
        check("GET after DEL", null, c.cmd("GET", "k"));
        check("INCR new", 1L, c.cmd("INCR", "n"));
        check("INCRBY", 11L, c.cmd("INCRBY", "n", "10"));
        check("DECR", 10L, c.cmd("DECR", "n"));
        check("DECRBY", 7L, c.cmd("DECRBY", "n", "3"));
        c.cmd("SET", "s", "abc");
        check("INCR non-int", "ERR value is not an integer or out of range", c.cmd("INCR", "s"));
        check("APPEND", 6L, c.cmd("APPEND", "s", "def"));
        check("STRLEN", 6L, c.cmd("STRLEN", "s"));
        check("MSET", "OK", c.cmd("MSET", "a", "1", "b", "2"));
        check("MGET", List.of("1", "2"), c.cmd("MGET", "a", "b"));
        check("SET bad opt", "ERR syntax error", c.cmd("SET", "k", "v", "BOGUS"));
    }

    static void expiry(Client c) throws Exception {
        check("SET PX", "OK", c.cmd("SET", "tmp", "v", "PX", "100"));
        check("GET before expiry", "v", c.cmd("GET", "tmp"));
        long pttl = (Long) c.cmd("PTTL", "tmp");
        check("PTTL in range", true, pttl > 0 && pttl <= 100);
        Thread.sleep(150);
        check("GET after expiry", null, c.cmd("GET", "tmp"));
        check("TTL missing", -2L, c.cmd("TTL", "tmp"));

        c.cmd("SET", "e", "v", "EX", "100");
        check("TTL", 100L, c.cmd("TTL", "e"));
        check("PERSIST", 1L, c.cmd("PERSIST", "e"));
        check("TTL persisted", -1L, c.cmd("TTL", "e"));
        check("EXPIRE", 1L, c.cmd("PEXPIRE", "e", "50"));
        Thread.sleep(300);
        check("expired key gone from KEYS", false, ((List<?>) c.cmd("KEYS", "*")).contains("e"));
        check("EXISTS after expiry", 0L, c.cmd("EXISTS", "e"));
        check("SET EX 0", "ERR invalid expire time in 'set' command", c.cmd("SET", "k", "v", "EX", "0"));
    }

    static void lists(Client c) throws IOException {
        check("RPUSH", 3L, c.cmd("RPUSH", "l", "a", "b", "c"));
        check("LPUSH", 4L, c.cmd("LPUSH", "l", "z"));
        check("LRANGE all", List.of("z", "a", "b", "c"), c.cmd("LRANGE", "l", "0", "-1"));
        check("LRANGE slice", List.of("a", "b"), c.cmd("LRANGE", "l", "1", "2"));
        check("LRANGE out of range", List.of(), c.cmd("LRANGE", "l", "10", "20"));
        check("LLEN", 4L, c.cmd("LLEN", "l"));
        check("LPOP", "z", c.cmd("LPOP", "l"));
        check("RPOP count", List.of("c", "b"), c.cmd("RPOP", "l", "2"));
        check("TYPE list", "list", c.cmd("TYPE", "l"));
        check("WRONGTYPE", "WRONGTYPE Operation against a key holding the wrong kind of value", c.cmd("GET", "l"));
        c.cmd("LPOP", "l");
        check("empty list removed", 0L, c.cmd("EXISTS", "l"));
    }

    static void transactions(Client c, int port) throws Exception {
        check("EXEC without MULTI", "ERR EXEC without MULTI", c.cmd("EXEC"));
        check("MULTI", "OK", c.cmd("MULTI"));
        check("queued SET", "QUEUED", c.cmd("SET", "tx", "1"));
        check("queued INCR", "QUEUED", c.cmd("INCR", "tx"));
        // Another client must not see the queued writes yet.
        try (Client other = new Client(port)) {
            check("isolation", null, other.cmd("GET", "tx"));
        }
        check("EXEC", List.of("OK", 2L), c.cmd("EXEC"));
        check("after EXEC", "2", c.cmd("GET", "tx"));

        c.cmd("MULTI");
        c.cmd("SET", "tx", "99");
        check("DISCARD", "OK", c.cmd("DISCARD"));
        check("after DISCARD", "2", c.cmd("GET", "tx"));

        c.cmd("MULTI");
        check("queue error", "ERR unknown command 'BOGUS'", c.cmd("BOGUS"));
        check("EXECABORT", "EXECABORT Transaction discarded because of previous errors.", c.cmd("EXEC"));
    }

    static void misc(Client c, String tmp) throws IOException {
        check("CONFIG GET", List.of("dir", tmp), c.cmd("CONFIG", "GET", "dir"));
        c.cmd("FLUSHALL");
        c.cmd("MSET", "user:1", "a", "user:2", "b", "post:1", "c");
        List<?> keys = (List<?>) c.cmd("KEYS", "user:*");
        check("KEYS glob", true, keys.size() == 2 && keys.contains("user:1") && keys.contains("user:2"));
        check("KEYS ?", 1L, (long) ((List<?>) c.cmd("KEYS", "post:?")).size());
        check("DBSIZE", 3L, c.cmd("DBSIZE"));
    }

    static void pipelining(int port) throws IOException {
        try (Socket s = new Socket("localhost", port)) {
            OutputStream out = s.getOutputStream();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) sb.append("*1\r\n$4\r\nPING\r\n");
            out.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Client.Reader r = new Client.Reader(new BufferedInputStream(s.getInputStream()));
            int pongs = 0;
            for (int i = 0; i < 100; i++) if ("PONG".equals(r.read())) pongs++;
            check("pipelined 100 PINGs", 100, pongs);
        }
    }

    static void concurrentClients(int port) throws Exception {
        int clients = 50, incrs = 200;
        CountDownLatch done = new CountDownLatch(clients);
        AtomicReference<Throwable> err = new AtomicReference<>();
        for (int i = 0; i < clients; i++) {
            Thread.ofVirtual().start(() -> {
                try (Client c = new Client(port)) {
                    for (int j = 0; j < incrs; j++) c.cmd("INCR", "counter");
                } catch (Throwable t) {
                    err.set(t);
                } finally {
                    done.countDown();
                }
            });
        }
        done.await();
        try (Client c = new Client(port)) {
            check("no lost updates across 50 clients", String.valueOf(clients * incrs), c.cmd("GET", "counter"));
        }
        check("no client errors", null, err.get());
    }

    static void inlineCommands(int port) throws IOException {
        try (Socket s = new Socket("localhost", port)) {
            s.getOutputStream().write("SET inline yes\r\nGET inline\r\n".getBytes(StandardCharsets.US_ASCII));
            Client.Reader r = new Client.Reader(new BufferedInputStream(s.getInputStream()));
            check("inline SET", "OK", r.read());
            check("inline GET", "yes", r.read());
        }
    }

    static void check(String name, Object expected, Object actual) {
        if (Objects.equals(expected, actual)) {
            passed++;
            System.out.println("  ok   " + name);
        } else {
            failed++;
            System.out.println("  FAIL " + name + ": expected <" + expected + "> but got <" + actual + ">");
        }
    }

    /** Minimal RESP client. Errors come back as their message string. */
    static final class Client implements AutoCloseable {
        private final Socket socket;
        private final OutputStream out;
        private final Reader reader;

        Client(int port) throws IOException {
            socket = new Socket("localhost", port);
            out = socket.getOutputStream();
            reader = new Reader(new BufferedInputStream(socket.getInputStream()));
        }

        Object cmd(String... args) throws IOException {
            StringBuilder sb = new StringBuilder("*").append(args.length).append("\r\n");
            for (String a : args) {
                sb.append('$').append(a.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(a).append("\r\n");
            }
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            return reader.read();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }

        static final class Reader {
            private final InputStream in;

            Reader(InputStream in) {
                this.in = in;
            }

            Object read() throws IOException {
                int type = in.read();
                String line = line();
                return switch (type) {
                    case '+', '-' -> line;
                    case ':' -> Long.parseLong(line);
                    case '$' -> {
                        int len = Integer.parseInt(line);
                        if (len < 0) yield null;
                        byte[] b = in.readNBytes(len);
                        in.readNBytes(2);
                        yield new String(b, StandardCharsets.UTF_8);
                    }
                    case '*' -> {
                        int n = Integer.parseInt(line);
                        if (n < 0) yield null;
                        List<Object> items = new ArrayList<>();
                        for (int i = 0; i < n; i++) items.add(read());
                        yield items;
                    }
                    default -> throw new IOException("bad reply type: " + (char) type);
                };
            }

            private String line() throws IOException {
                StringBuilder sb = new StringBuilder();
                int b;
                while ((b = in.read()) != '\r') {
                    if (b == -1) throw new IOException("eof");
                    sb.append((char) b);
                }
                in.read();
                return sb.toString();
            }
        }
    }
}
