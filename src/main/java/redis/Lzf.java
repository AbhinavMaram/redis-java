package redis;

/**
 * LZF decompression. Redis compresses strings longer than 20 bytes in RDB
 * files by default (rdbcompression yes), so a real dump.rdb is full of these.
 *
 * The stream is a sequence of chunks, each starting with a control byte:
 *   000LLLLL                  literal run: copy the next L+1 bytes as-is
 *   LLLooooo [LLLLLLLL] oooooooo
 *                             back-reference: copy L+2 bytes starting
 *                             (o + 1) bytes back in the output. If the 3-bit
 *                             L is 7, an extra length byte follows.
 */
final class Lzf {
    private Lzf() {}

    static byte[] decompress(byte[] in, int outLen) throws Rdb.RdbException {
        byte[] out = new byte[outLen];
        int ip = 0, op = 0;
        try {
            while (ip < in.length) {
                int ctrl = in[ip++] & 0xFF;
                if (ctrl < 32) {
                    int len = ctrl + 1;
                    System.arraycopy(in, ip, out, op, len);
                    ip += len;
                    op += len;
                } else {
                    int len = ctrl >> 5;
                    if (len == 7) len += in[ip++] & 0xFF;
                    int ref = op - ((ctrl & 0x1F) << 8) - (in[ip++] & 0xFF) - 1;
                    if (ref < 0) throw new Rdb.RdbException("LZF back-reference before start of output");
                    len += 2;
                    // Byte-by-byte on purpose: source and destination may overlap (run-length style).
                    for (int i = 0; i < len; i++) out[op++] = out[ref++];
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new Rdb.RdbException("corrupt LZF data");
        }
        if (op != outLen) throw new Rdb.RdbException("LZF length mismatch: expected " + outLen + ", got " + op);
        return out;
    }
}
