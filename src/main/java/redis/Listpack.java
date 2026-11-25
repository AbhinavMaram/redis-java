package redis;

import java.nio.charset.StandardCharsets;
import java.util.Deque;

/**
 * Decodes a listpack, the compact array Redis 7 uses inside quicklist nodes.
 *
 * Layout: total-bytes (u32 LE), element-count (u16 LE), entries..., 0xFF.
 * Each entry is: encoding+data, then a "backlen" (the entry's size, written
 * so the list can be walked backwards). Encodings, by the first byte:
 *   0xxxxxxx           7-bit unsigned int
 *   10xxxxxx           string, 6-bit length
 *   110xxxxx + 1 byte  13-bit signed int
 *   1110xxxx + 1 byte  string, 12-bit length
 *   11110000 + 4 bytes string, 32-bit length
 *   11110001..0100     int16 / int24 / int32 / int64, little-endian
 */
final class Listpack {
    private Listpack() {}

    static void decodeInto(byte[] lp, Deque<String> out) throws Rdb.RdbException {
        int pos = 6; // skip header
        try {
            while (true) {
                int b = lp[pos] & 0xFF;
                if (b == 0xFF) return;
                int size; // encoding + data, excluding backlen
                String value;
                if ((b & 0x80) == 0) {
                    value = Integer.toString(b & 0x7F);
                    size = 1;
                } else if ((b & 0xC0) == 0x80) {
                    int len = b & 0x3F;
                    value = str(lp, pos + 1, len);
                    size = 1 + len;
                } else if ((b & 0xE0) == 0xC0) {
                    int v = ((b & 0x1F) << 8) | (lp[pos + 1] & 0xFF);
                    if (v >= 1 << 12) v -= 1 << 13; // sign-extend 13 bits
                    value = Integer.toString(v);
                    size = 2;
                } else if ((b & 0xF0) == 0xE0) {
                    int len = ((b & 0x0F) << 8) | (lp[pos + 1] & 0xFF);
                    value = str(lp, pos + 2, len);
                    size = 2 + len;
                } else if (b == 0xF0) {
                    int len = (int) le(lp, pos + 1, 4);
                    value = str(lp, pos + 5, len);
                    size = 5 + len;
                } else if (b >= 0xF1 && b <= 0xF4) {
                    int n = switch (b) {
                        case 0xF1 -> 2;
                        case 0xF2 -> 3;
                        case 0xF3 -> 4;
                        default -> 8;
                    };
                    long raw = le(lp, pos + 1, n);
                    int shift = 64 - 8 * n;
                    value = Long.toString((raw << shift) >> shift); // sign-extend
                    size = 1 + n;
                } else {
                    throw new Rdb.RdbException("bad listpack encoding 0x" + Integer.toHexString(b));
                }
                out.addLast(value);
                pos += size + backlenSize(size);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new Rdb.RdbException("corrupt listpack");
        }
    }

    private static int backlenSize(int size) {
        if (size <= 127) return 1;
        if (size < 16383) return 2;
        if (size < 2097151) return 3;
        if (size < 268435455) return 4;
        return 5;
    }

    private static long le(byte[] b, int off, int n) {
        long v = 0;
        for (int i = 0; i < n; i++) v |= (long) (b[off + i] & 0xFF) << (8 * i);
        return v;
    }

    private static String str(byte[] b, int off, int len) {
        if (off + len > b.length) throw new ArrayIndexOutOfBoundsException();
        return new String(b, off, len, StandardCharsets.UTF_8);
    }
}
