package redis;

/**
 * CRC-64/Jones, the checksum Redis puts at the end of every RDB file.
 * Reflected input and output, initial value 0, no final XOR.
 * Check value: crc64("123456789") = 0xe9c6d914c4b8d9ca.
 */
final class Crc64 {
    private Crc64() {}

    private static final long POLY_REFLECTED = 0x95AC9329AC4BC9B5L;
    private static final long[] TABLE = new long[256];

    static {
        for (int i = 0; i < 256; i++) {
            long crc = i;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 1) != 0 ? (crc >>> 1) ^ POLY_REFLECTED : crc >>> 1;
            }
            TABLE[i] = crc;
        }
    }

    static long update(long crc, byte[] data, int off, int len) {
        for (int i = off; i < off + len; i++) {
            crc = TABLE[(int) ((crc ^ data[i]) & 0xFF)] ^ (crc >>> 8);
        }
        return crc;
    }
}
