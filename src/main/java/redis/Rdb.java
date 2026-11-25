package redis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes Redis RDB snapshot files.
 *
 * File layout:
 *   "REDIS0011"                      magic + 4-digit version
 *   FA key value ...                 AUX metadata fields (redis-ver, ctime, ...)
 *   FE dbnum                         SELECTDB
 *   FB size expires                  RESIZEDB (hint only)
 *   [FC ms | FD sec] type key value  one entry per key, optional expiry first
 *   FF crc64                         EOF + 8-byte little-endian CRC64 (0 = not checked)
 *
 * We write strings (type 0) and plain lists (type 1), which every Redis
 * version can load. We read those plus Redis 7's quicklist-of-listpacks
 * lists (type 18), so a dump.rdb written by real Redis loads too.
 */
public final class Rdb {
    private Rdb() {}

    public static final int VERSION = 11;

    // Opcodes
    private static final int OP_SLOT_INFO = 0xF4, OP_IDLE = 0xF8, OP_FREQ = 0xF9, OP_AUX = 0xFA,
            OP_RESIZEDB = 0xFB, OP_EXPIRE_MS = 0xFC, OP_EXPIRE_SEC = 0xFD, OP_SELECTDB = 0xFE, OP_EOF = 0xFF;

    // Value types
    private static final int TYPE_STRING = 0, TYPE_LIST = 1, TYPE_LIST_QUICKLIST_2 = 18;

    // Quicklist node containers
    private static final int QUICKLIST_PLAIN = 1, QUICKLIST_PACKED = 2;

    /** One key as stored on disk. value is a String or an ArrayDeque<String>. */
    public record Item(String key, Object value, long expiresAt) {}

    public static final class RdbException extends IOException {
        public RdbException(String msg) {
            super(msg);
        }
    }

    // ---------------------------------------------------------------- files

    public static List<Item> readFile(Path file) throws IOException {
        return read(Files.readAllBytes(file));
    }

    /** Writes to a temp file and renames it, so a crash never leaves a half-written dump. */
    public static void writeFile(Path file, List<Item> items) throws IOException {
        Path dir = file.toAbsolutePath().getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, "temp-", ".rdb");
        try {
            Files.write(tmp, write(items));
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    // --------------------------------------------------------------- writing

    public static byte[] write(List<Item> items) {
        Writer w = new Writer();
        w.raw(String.format("REDIS%04d", VERSION).getBytes(StandardCharsets.US_ASCII));
        w.aux("redis-ver", "7.2.0");
        w.aux("redis-bits", "64");
        w.aux("ctime", Long.toString(System.currentTimeMillis() / 1000));

        long withExpiry = items.stream().filter(i -> i.expiresAt() != Store.NO_EXPIRY).count();
        w.u8(OP_SELECTDB);
        w.length(0);
        w.u8(OP_RESIZEDB);
        w.length(items.size());
        w.length(withExpiry);

        for (Item item : items) {
            if (item.expiresAt() != Store.NO_EXPIRY) {
                w.u8(OP_EXPIRE_MS);
                w.int64LE(item.expiresAt());
            }
            if (item.value() instanceof String s) {
                w.u8(TYPE_STRING);
                w.string(item.key());
                w.string(s);
            } else if (item.value() instanceof ArrayDeque<?> list) {
                w.u8(TYPE_LIST);
                w.string(item.key());
                w.length(list.size());
                for (Object o : list) w.string((String) o);
            } else {
                throw new IllegalArgumentException("cannot persist " + item.value().getClass());
            }
        }
        w.u8(OP_EOF);
        long crc = Crc64.update(0, w.buf.toByteArray(), 0, w.buf.size());
        w.int64LE(crc);
        return w.buf.toByteArray();
    }

    private static final class Writer {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        void u8(int b) {
            buf.write(b);
        }

        void raw(byte[] b) {
            buf.writeBytes(b);
        }

        void int64LE(long v) {
            for (int i = 0; i < 8; i++) buf.write((int) (v >>> (8 * i)) & 0xFF);
        }

        void length(long n) {
            if (n < 0x40) {
                buf.write((int) n);
            } else if (n < 0x4000) {
                buf.write(0x40 | (int) (n >> 8));
                buf.write((int) n & 0xFF);
            } else if (n <= 0xFFFFFFFFL) {
                buf.write(0x80);
                for (int i = 3; i >= 0; i--) buf.write((int) (n >>> (8 * i)) & 0xFF);
            } else {
                buf.write(0x81);
                for (int i = 7; i >= 0; i--) buf.write((int) (n >>> (8 * i)) & 0xFF);
            }
        }

        /** Like Redis, store short canonical integers ("42", "-7") as int8/16/32 instead of text. */
        void string(String s) {
            Long n = canonicalInt(s);
            if (n != null && n >= Byte.MIN_VALUE && n <= Byte.MAX_VALUE) {
                buf.write(0xC0);
                buf.write(n.intValue() & 0xFF);
            } else if (n != null && n >= Short.MIN_VALUE && n <= Short.MAX_VALUE) {
                buf.write(0xC1);
                buf.write(n.intValue() & 0xFF);
                buf.write((n.intValue() >> 8) & 0xFF);
            } else if (n != null && n >= Integer.MIN_VALUE && n <= Integer.MAX_VALUE) {
                buf.write(0xC2);
                for (int i = 0; i < 4; i++) buf.write((n.intValue() >> (8 * i)) & 0xFF);
            } else {
                byte[] b = s.getBytes(StandardCharsets.UTF_8);
                length(b.length);
                buf.writeBytes(b);
            }
        }

        void aux(String k, String v) {
            u8(OP_AUX);
            string(k);
            string(v);
        }

        private static Long canonicalInt(String s) {
            if (s.isEmpty() || s.length() > 11) return null;
            try {
                long n = Long.parseLong(s);
                return Long.toString(n).equals(s) ? n : null; // rejects "007", "+1", "-0"
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    // --------------------------------------------------------------- reading

    /** Parses an RDB file. Keys outside db 0 are skipped; keys already expired are dropped by the caller. */
    public static List<Item> read(byte[] data) throws RdbException {
        Parser p = new Parser(data);
        String magic = new String(p.bytes(5), StandardCharsets.US_ASCII);
        if (!magic.equals("REDIS")) throw new RdbException("not an RDB file (bad magic)");
        int version;
        try {
            version = Integer.parseInt(new String(p.bytes(4), StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            throw new RdbException("bad RDB version");
        }
        if (version < 1 || version > 12) throw new RdbException("unsupported RDB version " + version);

        List<Item> items = new ArrayList<>();
        long db = 0;
        long expiresAt = Store.NO_EXPIRY;

        while (true) {
            int op = p.u8();
            switch (op) {
                case OP_EOF -> {
                    if (version >= 5) verifyChecksum(data, p.pos);
                    return items;
                }
                case OP_AUX -> {
                    p.string();
                    p.string();
                }
                case OP_SELECTDB -> db = p.length();
                case OP_RESIZEDB -> {
                    p.length();
                    p.length();
                }
                case OP_SLOT_INFO -> {
                    p.length();
                    p.length();
                    p.length();
                }
                case OP_EXPIRE_MS -> expiresAt = p.int64LE();
                case OP_EXPIRE_SEC -> expiresAt = p.u32LE() * 1000L;
                case OP_IDLE -> p.length();
                case OP_FREQ -> p.u8();
                default -> {
                    // Anything else is a value type, followed by key and value.
                    String key = utf8(p.string());
                    Object value = readValue(p, op);
                    if (db == 0) items.add(new Item(key, value, expiresAt));
                    expiresAt = Store.NO_EXPIRY;
                }
            }
        }
    }

    private static Object readValue(Parser p, int type) throws RdbException {
        switch (type) {
            case TYPE_STRING:
                return utf8(p.string());
            case TYPE_LIST: {
                long n = p.length();
                ArrayDeque<String> list = new ArrayDeque<>();
                for (long i = 0; i < n; i++) list.addLast(utf8(p.string()));
                return list;
            }
            case TYPE_LIST_QUICKLIST_2: {
                long nodes = p.length();
                ArrayDeque<String> list = new ArrayDeque<>();
                for (long i = 0; i < nodes; i++) {
                    long container = p.length();
                    byte[] blob = p.string();
                    if (container == QUICKLIST_PLAIN) list.addLast(utf8(blob));
                    else if (container == QUICKLIST_PACKED) Listpack.decodeInto(blob, list);
                    else throw new RdbException("bad quicklist container " + container);
                }
                return list;
            }
            default:
                throw new RdbException("unsupported RDB value type " + type
                        + " (only strings and lists are supported)");
        }
    }

    private static void verifyChecksum(byte[] data, int eofEnd) throws RdbException {
        if (data.length < eofEnd + 8) throw new RdbException("truncated RDB file (missing checksum)");
        long expected = 0;
        for (int i = 7; i >= 0; i--) expected = (expected << 8) | (data[eofEnd + i] & 0xFF);
        if (expected == 0) return; // saved with rdbchecksum no
        long actual = Crc64.update(0, data, 0, eofEnd);
        if (actual != expected) {
            throw new RdbException(String.format("RDB checksum mismatch: file says %016x, computed %016x",
                    expected, actual));
        }
    }

    private static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    /** Cursor over the file bytes with RDB's primitive decoders. */
    private static final class Parser {
        final byte[] d;
        int pos;

        Parser(byte[] d) {
            this.d = d;
        }

        int u8() throws RdbException {
            need(1);
            return d[pos++] & 0xFF;
        }

        byte[] bytes(int n) throws RdbException {
            need(n);
            byte[] out = java.util.Arrays.copyOfRange(d, pos, pos + n);
            pos += n;
            return out;
        }

        long u32LE() throws RdbException {
            long v = 0;
            for (int i = 0; i < 4; i++) v |= (long) u8() << (8 * i);
            return v;
        }

        long int64LE() throws RdbException {
            long v = 0;
            for (int i = 0; i < 8; i++) v |= (long) u8() << (8 * i);
            return v;
        }

        long uBE(int n) throws RdbException {
            long v = 0;
            for (int i = 0; i < n; i++) v = (v << 8) | u8();
            return v;
        }

        /**
         * Length encoding, by the top two bits of the first byte:
         *   00xxxxxx            6-bit length
         *   01xxxxxx xxxxxxxx   14-bit length (big-endian)
         *   10000000 + 4 bytes  32-bit length (big-endian)
         *   10000001 + 8 bytes  64-bit length (big-endian)
         *   11xxxxxx            special string encoding (handled in string())
         */
        long length() throws RdbException {
            int b = u8();
            return switch (b >> 6) {
                case 0 -> b & 0x3F;
                case 1 -> ((b & 0x3F) << 8) | u8();
                case 2 -> {
                    if (b == 0x80) yield uBE(4);
                    if (b == 0x81) yield uBE(8);
                    throw new RdbException("bad length encoding 0x" + Integer.toHexString(b));
                }
                default -> throw new RdbException("expected a length, got special encoding");
            };
        }

        /** A string: raw bytes, an integer stored compactly, or an LZF-compressed blob. */
        byte[] string() throws RdbException {
            need(1);
            int b = d[pos] & 0xFF;
            if ((b >> 6) != 3) {
                long len = length();
                if (len > Integer.MAX_VALUE) throw new RdbException("string too long");
                return bytes((int) len);
            }
            pos++;
            int enc = b & 0x3F;
            if (enc == 3) {
                long clen = length(), len = length();
                if (clen > Integer.MAX_VALUE || len > Integer.MAX_VALUE) throw new RdbException("LZF blob too long");
                return Lzf.decompress(bytes((int) clen), (int) len);
            }
            long n = switch (enc) {
                case 0 -> (byte) u8();                    // int8
                case 1 -> (short) (u8() | (u8() << 8));   // int16, little-endian
                case 2 -> (int) u32LE();                  // int32, little-endian
                default -> throw new RdbException("bad string encoding " + enc);
            };
            return Long.toString(n).getBytes(StandardCharsets.US_ASCII);
        }

        void need(int n) throws RdbException {
            if (pos + n > d.length) throw new RdbException("unexpected end of RDB file");
        }
    }
}
