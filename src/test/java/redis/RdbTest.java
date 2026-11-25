package redis;

import static redis.ServerTest.check;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for the RDB format and for persistence through the running server. */
public final class RdbTest {
    public static void main(String[] args) throws Exception {
        primitives();
        handBuiltFile();
        roundTrip();
        corruption();
        serverPersistence();
        ServerTest.summary();
    }

    static void primitives() throws Exception {
        byte[] check = "123456789".getBytes(StandardCharsets.US_ASCII);
        check("CRC64 check value", 0xe9c6d914c4b8d9caL, Crc64.update(0, check, 0, check.length));

        // literal "abc" (ctrl 2), then back-reference: copy 6 bytes from 3 back (ctrl 0x80, offset byte 2)
        byte[] lzf = {0x02, 'a', 'b', 'c', (byte) 0x80, 0x02};
        check("LZF overlapping back-reference", "abcabcabc", new String(Lzf.decompress(lzf, 9), StandardCharsets.US_ASCII));
    }

    /** A file built byte by byte, using encodings our writer never emits but real Redis does. */
    static void handBuiltFile() throws Exception {
        long future = System.currentTimeMillis() + 3_600_000;
        Bytes b = new Bytes();
        b.ascii("REDIS0011");
        b.u8(0xFA).str("redis-ver").str("7.2.4");
        b.u8(0xFA).str("redis-bits").u8(0xC0).u8(64);          // AUX value as int8
        b.u8(0xFE).u8(0);                                      // SELECTDB 0
        b.u8(0xFB).u8(9).u8(3);                                // RESIZEDB
        b.u8(0).str("plain").str("hello");
        b.u8(0xFC).le(future, 8).u8(0).str("ms-expiry").str("soon");
        b.u8(0xFC).le(1000, 8).u8(0).str("long-gone").str("x");      // expired in 1970
        b.u8(0xFD).le(future / 1000, 4).u8(0).str("sec-expiry").str("s");
        b.u8(0).str("int8").u8(0xC0).u8(0xF9);                 // -7
        b.u8(0).str("int16").u8(0xC1).le(1000, 2);
        b.u8(0).str("int32").u8(0xC2).le(-100000, 4);
        b.u8(0).str("lzf").u8(0xC3).u8(6).u8(9).raw(new byte[] {0x02, 'a', 'b', 'c', (byte) 0x80, 0x02});
        b.u8(0xF8).u8(5);                                      // IDLE hint, ignored
        b.u8(1).str("oldlist").u8(2).str("a").str("b");        // type 1: plain list

        // Type 18: quicklist of listpacks, the format Redis 7 writes for lists.
        String seventy = "x".repeat(70);
        byte[] lp = listpack(
                new byte[] {0x05},                                   // 7-bit uint 5
                new byte[] {(byte) 0x82, 'h', 'i'},                  // 6-bit string "hi"
                new byte[] {(byte) 0xDF, (byte) 0x9C},               // 13-bit int -100
                new byte[] {(byte) 0xF1, (byte) 0xE8, 0x03},         // int16 1000
                concat(new byte[] {(byte) 0xE0, 70}, seventy.getBytes(StandardCharsets.US_ASCII))); // 12-bit string
        b.u8(18).str("newlist").u8(2);                          // 2 nodes
        b.u8(2).len(lp.length).raw(lp);                         // packed node
        b.u8(1).str("plain-node");                              // plain node

        b.u8(0xFE).u8(1);                                       // SELECTDB 1: we only serve db 0
        b.u8(0).str("other-db").str("ignored");
        b.u8(0xFF).le(0, 8);                                    // EOF, checksum 0 = unchecked

        Store store = new Store();
        int n = store.load(Rdb.read(b.bytes()));
        check("hand-built: key count (expired + other db skipped)", 9, n);
        check("hand-built: string", "hello", store.getEntry("plain").value);
        check("hand-built: ms expiry kept", future, store.getEntry("ms-expiry").expiresAt);
        check("hand-built: sec expiry", future / 1000 * 1000, store.getEntry("sec-expiry").expiresAt);
        check("hand-built: expired key dropped", null, store.getEntry("long-gone"));
        check("hand-built: int8", "-7", store.getEntry("int8").value);
        check("hand-built: int16", "1000", store.getEntry("int16").value);
        check("hand-built: int32", "-100000", store.getEntry("int32").value);
        check("hand-built: LZF string", "abcabcabc", store.getEntry("lzf").value);
        check("hand-built: type-1 list", List.of("a", "b"), List.copyOf((ArrayDeque<?>) store.getEntry("oldlist").value));
        check("hand-built: quicklist + listpack", List.of("5", "hi", "-100", "1000", seventy, "plain-node"),
                List.copyOf((ArrayDeque<?>) store.getEntry("newlist").value));
        check("hand-built: other db ignored", null, store.getEntry("other-db"));
    }

    static void roundTrip() throws Exception {
        long exp = System.currentTimeMillis() + 60_000;
        ArrayDeque<String> list = new ArrayDeque<>(List.of("one", "2", "-3", "ünïcödé", ""));
        List<Rdb.Item> items = List.of(
                new Rdb.Item("s", "value", Store.NO_EXPIRY),
                new Rdb.Item("n", "12345", exp),
                new Rdb.Item("big", "9999999999", Store.NO_EXPIRY),    // too big for int32: stored as text
                new Rdb.Item("padded", "007", Store.NO_EXPIRY),       // not canonical: must stay "007"
                new Rdb.Item("long", "y".repeat(20_000), Store.NO_EXPIRY), // 32-bit length encoding
                new Rdb.Item("empty", "", Store.NO_EXPIRY),
                new Rdb.Item("l", list, exp));
        byte[] file = Rdb.write(items);
        List<Rdb.Item> back = Rdb.read(file);

        Map<String, Rdb.Item> byKey = new HashMap<>();
        for (Rdb.Item i : back) byKey.put(i.key(), i);
        check("round trip: count", items.size(), back.size());
        for (Rdb.Item i : items) {
            Rdb.Item got = byKey.get(i.key());
            Object want = i.value() instanceof ArrayDeque<?> d ? List.copyOf(d) : i.value();
            Object have = got == null ? null : got.value() instanceof ArrayDeque<?> d ? List.copyOf(d) : got.value();
            check("round trip: " + i.key(), want + "@" + i.expiresAt(), have + "@" + (got == null ? null : got.expiresAt()));
        }
        check("round trip: header", "REDIS0011", new String(file, 0, 9, StandardCharsets.US_ASCII));
    }

    static void corruption() {
        byte[] file = Rdb.write(List.of(new Rdb.Item("k", "value", Store.NO_EXPIRY)));
        file[file.length - 12] ^= 0x01; // flip a bit inside the data, before EOF + checksum
        check("bad checksum rejected", "checksum", failure(file));
        check("bad magic rejected", "magic", failure("NOTREDIS00".getBytes(StandardCharsets.US_ASCII)));
        byte[] truncated = java.util.Arrays.copyOf(Rdb.write(List.of(new Rdb.Item("k", "v", -1))), 15);
        check("truncated file rejected", "end of RDB", failure(truncated));
        Bytes hash = new Bytes().ascii("REDIS0011").u8(4).str("h"); // type 4 = hash
        check("unsupported type rejected", "unsupported RDB value type 4", failure(hash.bytes()));
    }

    static void serverPersistence() throws Exception {
        Path dir = Files.createTempDirectory("redis-rdb");
        Map<String, String> config = Map.of("dir", dir.toString(), "dbfilename", "dump.rdb", "save", "");

        // 1. Write data, SAVE, stop.
        try (RedisServer s = start(config); ServerTest.Client c = new ServerTest.Client(s.port())) {
            c.cmd("SET", "greeting", "hello");
            c.cmd("SET", "counter", "41");
            c.cmd("INCR", "counter");
            c.cmd("SET", "session", "abc", "EX", "100");
            c.cmd("RPUSH", "queue", "a", "b", "c");
            check("INFO dirty before save", true, info(c).contains("rdb_changes_since_last_save:5"));
            check("SAVE", "OK", c.cmd("SAVE"));
            check("INFO dirty after save", true, info(c).contains("rdb_changes_since_last_save:0"));
            check("dump.rdb written", true, Files.exists(dir.resolve("dump.rdb")));
            check("no temp files left", 1L, Files.list(dir).count());
        }

        // 2. Restart: everything comes back, TTLs included.
        try (RedisServer s = start(config); ServerTest.Client c = new ServerTest.Client(s.port())) {
            check("restart: string", "hello", c.cmd("GET", "greeting"));
            check("restart: counter", "42", c.cmd("GET", "counter"));
            long ttl = (Long) c.cmd("TTL", "session");
            check("restart: TTL survives", true, ttl > 90 && ttl <= 100);
            check("restart: list", List.of("a", "b", "c"), c.cmd("LRANGE", "queue", "0", "-1"));
            check("restart: no TTL on plain key", -1L, c.cmd("TTL", "greeting"));

            // 3. BGSAVE
            c.cmd("SET", "later", "yes");
            check("BGSAVE", "Background saving started", c.cmd("BGSAVE"));
            waitFor(() -> info(c).contains("rdb_bgsave_in_progress:0"));
            check("LASTSAVE is recent", true, Math.abs((Long) c.cmd("LASTSAVE") - System.currentTimeMillis() / 1000) <= 2);
        }
        try (RedisServer s = start(config); ServerTest.Client c = new ServerTest.Client(s.port())) {
            check("BGSAVE persisted", "yes", c.cmd("GET", "later"));
        }

        // 4. Save points: "1 1" = snapshot once 1 write is at least 1 second old.
        Path dir2 = Files.createTempDirectory("redis-rdb-auto");
        Map<String, String> auto = Map.of("dir", dir2.toString(), "dbfilename", "auto.rdb", "save", "1 1");
        try (RedisServer s = start(auto); ServerTest.Client c = new ServerTest.Client(s.port())) {
            c.cmd("SET", "auto", "saved");
            check("CONFIG GET save", List.of("save", "1 1"), c.cmd("CONFIG", "GET", "save"));
            waitFor(() -> Files.exists(dir2.resolve("auto.rdb")));
            check("save point triggered BGSAVE", true, Files.exists(dir2.resolve("auto.rdb")));
        }
        try (RedisServer s = start(auto); ServerTest.Client c = new ServerTest.Client(s.port())) {
            check("auto-saved data loads", "saved", c.cmd("GET", "auto"));
        }

        // 5. SHUTDOWN saves when asked to (the exit itself is replaced by a no-op here).
        Path dir3 = Files.createTempDirectory("redis-rdb-shutdown");
        Map<String, String> sd = Map.of("dir", dir3.toString(), "dbfilename", "dump.rdb", "save", "");
        try (RedisServer s = start(sd); ServerTest.Client c = new ServerTest.Client(s.port())) {
            c.cmd("SET", "k", "v");
            c.cmd("SHUTDOWN", "NOSAVE");
            check("SHUTDOWN NOSAVE writes nothing", false, Files.exists(dir3.resolve("dump.rdb")));
            c.cmd("SHUTDOWN", "SAVE");
            check("SHUTDOWN SAVE writes dump", true, Files.exists(dir3.resolve("dump.rdb")));
        }

        // 6. A corrupt dump stops the server from starting instead of being overwritten.
        Path dir4 = Files.createTempDirectory("redis-rdb-corrupt");
        Files.write(dir4.resolve("dump.rdb"), "REDIS0011garbage".getBytes(StandardCharsets.US_ASCII));
        String msg;
        try (RedisServer s = start(Map.of("dir", dir4.toString(), "dbfilename", "dump.rdb", "save", ""))) {
            msg = "started";
        } catch (java.io.IOException e) {
            msg = e.getMessage();
        }
        check("corrupt dump refuses to start", true, msg.startsWith("failed to load"));
    }

    // ------------------------------------------------------------- helpers

    static RedisServer start(Map<String, String> config) throws Exception {
        RedisServer s = new RedisServer(0, config);
        Thread t = new Thread(s::serve);
        t.setDaemon(true);
        t.start();
        return s;
    }

    static String info(ServerTest.Client c) {
        try {
            return (String) c.cmd("INFO");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    interface Cond {
        boolean ok() throws Exception;
    }

    static void waitFor(Cond cond) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (!cond.ok() && System.currentTimeMillis() < deadline) Thread.sleep(50);
    }

    static String failure(byte[] file) {
        try {
            Rdb.read(file);
            return "no error";
        } catch (Exception e) {
            for (String want : List.of("checksum", "magic", "end of RDB", "unsupported RDB value type 4")) {
                if (e.getMessage().contains(want)) return want;
            }
            return e.getMessage();
        }
    }

    static byte[] listpack(byte[]... entries) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] e : entries) {
            body.writeBytes(e);
            body.write(e.length); // backlen: every entry here is <= 127 bytes, so 1 byte
        }
        int total = 4 + 2 + body.size() + 1;
        Bytes lp = new Bytes().le(total, 4).le(entries.length, 2).raw(body.toByteArray()).u8(0xFF);
        return lp.bytes();
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] out = java.util.Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    static final class Bytes {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        Bytes u8(int b) {
            out.write(b);
            return this;
        }

        Bytes raw(byte[] b) {
            out.writeBytes(b);
            return this;
        }

        Bytes ascii(String s) {
            return raw(s.getBytes(StandardCharsets.US_ASCII));
        }

        Bytes le(long v, int n) {
            for (int i = 0; i < n; i++) out.write((int) (v >>> (8 * i)) & 0xFF);
            return this;
        }

        Bytes len(int n) {
            if (n < 64) return u8(n);
            return u8(0x40 | (n >> 8)).u8(n & 0xFF);
        }

        Bytes str(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            return len(b.length).raw(b);
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }
}
