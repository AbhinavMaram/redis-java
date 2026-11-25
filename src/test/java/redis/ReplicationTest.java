package redis;

import static redis.RdbTest.waitFor;
import static redis.ServerTest.check;

import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Master/replica tests: real servers, plus fake peers that check exact bytes on the wire. */
public final class ReplicationTest {
    public static void main(String[] args) throws Exception {
        masterAndReplicas();
        masterWireProtocol();
        replicaAgainstFakeMaster();
        replicaReconnects();
        ServerTest.summary();
    }

    static void masterAndReplicas() throws Exception {
        try (RedisServer master = start(config(null));
             ServerTest.Client m = new ServerTest.Client(master.port())) {
            m.cmd("SET", "before", "sync");
            m.cmd("RPUSH", "list", "a", "b");
            check("WAIT with no replicas", 0L, m.cmd("WAIT", "1", "100"));

            try (RedisServer r1 = start(config("localhost " + master.port()));
                 RedisServer r2 = start(config("localhost " + master.port()));
                 ServerTest.Client c1 = new ServerTest.Client(r1.port());
                 ServerTest.Client c2 = new ServerTest.Client(r2.port())) {
                waitFor(() -> info(c1).contains("master_link_status:up") && info(c2).contains("master_link_status:up"));
                check("replica link up", true, info(c1).contains("master_link_status:up"));

                // Full resync carried the existing data.
                check("snapshot: string", "sync", c1.cmd("GET", "before"));
                check("snapshot: list", List.of("a", "b"), c2.cmd("LRANGE", "list", "0", "-1"));

                String mInfo = info(m);
                check("master INFO role", true, mInfo.contains("role:master"));
                check("master INFO connected_slaves", true, mInfo.contains("connected_slaves:2"));
                String replId = field(mInfo, "master_replid");
                check("replid is 40 hex chars", true, replId.matches("[0-9a-f]{40}"));
                check("replica INFO role", true, info(c1).contains("role:slave"));
                check("replica knows master replid", replId, field(info(c1), "master_replid"));

                check("WAIT before any new write returns all replicas", 2L, m.cmd("WAIT", "2", "1000"));

                // Writes stream to both replicas.
                m.cmd("SET", "k", "v");
                m.cmd("INCR", "n");
                m.cmd("INCR", "n");
                m.cmd("RPUSH", "list", "c");
                m.cmd("DEL", "before");
                m.cmd("SET", "skipped", "x", "XX"); // didn't write: must not reach replicas as a SET
                check("WAIT 2", 2L, m.cmd("WAIT", "2", "2000"));
                for (ServerTest.Client c : List.of(c1, c2)) {
                    check("replicated SET", "v", c.cmd("GET", "k"));
                    check("replicated INCR", "2", c.cmd("GET", "n"));
                    check("replicated RPUSH", List.of("a", "b", "c"), c.cmd("LRANGE", "list", "0", "-1"));
                    check("replicated DEL", 0L, c.cmd("EXISTS", "before"));
                    check("skipped SET XX", 0L, c.cmd("EXISTS", "skipped"));
                }

                // Expiry is sent as an absolute time, so replica and master agree.
                m.cmd("SET", "ttl", "v", "PX", "60000");
                m.cmd("SET", "e", "v");
                m.cmd("EXPIRE", "e", "100");
                m.cmd("SET", "gone", "v");
                m.cmd("EXPIRE", "gone", "0"); // deletes on master, so replicas get DEL
                m.cmd("WAIT", "2", "2000");
                long mp = (Long) m.cmd("PTTL", "ttl"), rp = (Long) c1.cmd("PTTL", "ttl");
                check("PX replicated as same deadline", true, Math.abs(mp - rp) < 50);
                long et = (Long) c1.cmd("TTL", "e");
                check("EXPIRE replicated", true, et > 95 && et <= 100);
                check("EXPIRE 0 replicated as delete", 0L, c1.cmd("EXISTS", "gone"));

                // Transactions arrive as MULTI ... EXEC and apply together.
                m.cmd("MULTI");
                m.cmd("SET", "tx1", "a");
                m.cmd("SET", "tx2", "b");
                m.cmd("EXEC");
                m.cmd("WAIT", "2", "2000");
                check("replicated transaction", List.of("a", "b"), c2.cmd("MGET", "tx1", "tx2"));

                // Replicas are read-only for normal clients.
                check("replica rejects writes", "READONLY You can't write against a read only replica.",
                        c1.cmd("SET", "x", "y"));
                check("replica still serves reads", "v", c1.cmd("GET", "k"));
                check("WAIT on replica", "ERR WAIT cannot be used with replica instances.", c1.cmd("WAIT", "1", "0"));

                // WAIT for more replicas than exist: times out, returns how many acked.
                m.cmd("SET", "k", "v2");
                long t0 = System.currentTimeMillis();
                check("WAIT 3 with 2 replicas", 2L, m.cmd("WAIT", "3", "300"));
                long waited = System.currentTimeMillis() - t0;
                check("WAIT honoured its timeout", true, waited >= 250 && waited < 2000);

                // Offsets converge once everything is acknowledged.
                waitFor(() -> field(info(c1), "slave_repl_offset").equals(field(info(m), "master_repl_offset")));
                check("replica offset == master offset", field(info(m), "master_repl_offset"),
                        field(info(c1), "slave_repl_offset"));
            }

            waitFor(() -> info(m).contains("connected_slaves:0"));
            check("master notices replicas leaving", true, info(m).contains("connected_slaves:0"));
        }
    }

    /** Acts as a replica by hand and checks exactly what the master sends. */
    static void masterWireProtocol() throws Exception {
        try (RedisServer master = start(config(null));
             ServerTest.Client m = new ServerTest.Client(master.port());
             Socket s = new Socket("localhost", master.port())) {
            m.cmd("SET", "seed", "1");
            RespReader in = new RespReader(new BufferedInputStream(s.getInputStream()));
            OutputStream out = s.getOutputStream();

            send(out, "PING");
            check("wire: PING", "+PONG", in.readSimpleReply());
            send(out, "REPLCONF", "listening-port", "7777");
            check("wire: REPLCONF listening-port", "+OK", in.readSimpleReply());
            send(out, "REPLCONF", "capa", "psync2");
            check("wire: REPLCONF capa", "+OK", in.readSimpleReply());
            send(out, "PSYNC", "?", "-1");
            String resync = in.readSimpleReply();
            check("wire: FULLRESYNC <replid> 0", true, resync.matches("\\+FULLRESYNC [0-9a-f]{40} 0"));
            byte[] rdb = in.readRdbTransfer();
            check("wire: RDB payload", "REDIS0011", new String(rdb, 0, 9, StandardCharsets.US_ASCII));
            check("wire: RDB has the seed key", "1", Rdb.read(rdb).get(0).value());

            m.cmd("SET", "foo", "bar", "EX", "100");
            List<String> cmd = in.readCommand();
            check("wire: SET EX sent as PXAT", true, cmd.size() == 5 && cmd.subList(0, 4).equals(
                    List.of("SET", "foo", "bar", "PXAT")) && Long.parseLong(cmd.get(4)) > System.currentTimeMillis());
            check("wire: INFO lists replica port", true, info(m).contains("port=7777"));

            // WAIT makes the master send GETACK. We answer with the offset we've seen so far.
            long setBytes = Resp.bulkArray(cmd).getBytes(StandardCharsets.UTF_8).length;
            Thread waiter = Thread.ofVirtual().start(() -> {
                try (ServerTest.Client w = new ServerTest.Client(master.port())) {
                    check("wire: WAIT counts our ACK", 1L, w.cmd("WAIT", "1", "5000"));
                } catch (Exception e) {
                    check("wire: WAIT", "no exception", e.toString());
                }
            });
            check("wire: GETACK", List.of("REPLCONF", "GETACK", "*"), in.readCommand());
            send(out, "REPLCONF", "ACK", Long.toString(setBytes));
            waiter.join(6000);
            check("wire: master offset counts bytes", true,
                    info(m).contains("master_repl_offset:" + (setBytes + 37))); // 37 = GETACK's size
        }
    }

    /** A fake master: checks the replica's handshake and its byte-exact ACK offsets. */
    static void replicaAgainstFakeMaster() throws Exception {
        try (ServerSocket fake = new ServerSocket(0)) {
            try (RedisServer replica = start(config("localhost " + fake.getLocalPort()));
                 Socket s = fake.accept()) {
                RespReader in = new RespReader(new BufferedInputStream(s.getInputStream()));
                OutputStream out = s.getOutputStream();

                check("handshake 1: PING", List.of("PING"), in.readCommand());
                raw(out, "+PONG\r\n");
                check("handshake 2: listening-port", List.of("REPLCONF", "listening-port", Integer.toString(replica.port())),
                        in.readCommand());
                raw(out, "+OK\r\n");
                check("handshake 3: capa", List.of("REPLCONF", "capa", "psync2"), in.readCommand());
                raw(out, "+OK\r\n");
                check("handshake 4: PSYNC", List.of("PSYNC", "?", "-1"), in.readCommand());
                byte[] rdb = Rdb.write(List.of(new Rdb.Item("from-rdb", "hi", Store.NO_EXPIRY)));
                raw(out, "+FULLRESYNC " + "ab".repeat(20) + " 1000\r\n\n\n$" + rdb.length + "\r\n"); // \n = keepalives
                out.write(rdb);

                String set = Resp.bulkArray(List.of("SET", "foo", "123"));
                String ping = Resp.bulkArray(List.of("PING"));
                String getack = Resp.bulkArray(List.of("REPLCONF", "GETACK", "*"));
                raw(out, set + ping + getack);
                long expected = 1000 + set.length() + ping.length();
                check("ACK = offset before GETACK", List.of("REPLCONF", "ACK", Long.toString(expected)), in.readCommand());
                raw(out, getack);
                check("ACK counts the previous GETACK", List.of("REPLCONF", "ACK", Long.toString(expected + getack.length())),
                        in.readCommand());

                try (ServerTest.Client c = new ServerTest.Client(replica.port())) {
                    check("replica loaded fake master's RDB", "hi", c.cmd("GET", "from-rdb"));
                    check("replica applied streamed SET", "123", c.cmd("GET", "foo"));
                    check("replica sent no reply for SET/PING", 0, s.getInputStream().available());
                    check("replica INFO master_replid", "ab".repeat(20), field(info(c), "master_replid"));
                }
            }
        }
    }

    /** If the master goes away, the replica keeps retrying and resyncs when it's back. */
    static void replicaReconnects() throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        RedisServer master = startOn(port, config(null));
        try (RedisServer replica = start(config("localhost " + port));
             ServerTest.Client r = new ServerTest.Client(replica.port())) {
            waitFor(() -> info(r).contains("master_link_status:up"));
            master.close();
            waitFor(() -> info(r).contains("master_link_status:down"));
            check("link down after master stops", true, info(r).contains("master_link_status:down"));

            master = startOn(port, config(null));
            try (ServerTest.Client m = new ServerTest.Client(port)) {
                m.cmd("SET", "after", "restart");
            }
            waitFor(() -> "restart".equals(r.cmd("GET", "after")));
            check("replica resynced with restarted master", "restart", r.cmd("GET", "after"));
        } finally {
            master.close();
        }
    }

    // ------------------------------------------------------------- helpers

    static Map<String, String> config(String replicaOf) throws Exception {
        Map<String, String> c = new HashMap<>();
        c.put("dir", Files.createTempDirectory("redis-repl").toString());
        c.put("dbfilename", "dump.rdb");
        c.put("save", "");
        if (replicaOf != null) c.put("replicaof", replicaOf);
        return c;
    }

    static RedisServer start(Map<String, String> config) throws Exception {
        return startOn(0, config);
    }

    static RedisServer startOn(int port, Map<String, String> config) throws Exception {
        RedisServer s = new RedisServer(port, config);
        Thread t = new Thread(s::serve);
        t.setDaemon(true);
        t.start();
        return s;
    }

    static String info(ServerTest.Client c) {
        return RdbTest.info(c);
    }

    static String field(String info, String name) {
        for (String line : info.split("\r\n")) if (line.startsWith(name + ":")) return line.substring(name.length() + 1);
        return null;
    }

    static void send(OutputStream out, String... cmd) throws Exception {
        raw(out, Resp.bulkArray(List.of(cmd)));
    }

    static void raw(OutputStream out, String s) throws Exception {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
