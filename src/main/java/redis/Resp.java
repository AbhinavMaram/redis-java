package redis;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Builds RESP2 replies. Every method returns a complete, wire-ready reply. */
public final class Resp {
    private Resp() {}

    public static final String OK = simple("OK");
    public static final String NULL_BULK = "$-1\r\n";
    public static final String NULL_ARRAY = "*-1\r\n";

    public static String simple(String s) {
        return "+" + s + "\r\n";
    }

    public static String error(String msg) {
        return "-" + msg + "\r\n";
    }

    public static String integer(long n) {
        return ":" + n + "\r\n";
    }

    public static String bulk(String s) {
        if (s == null) return NULL_BULK;
        // Length is in bytes, not characters.
        int len = s.getBytes(StandardCharsets.UTF_8).length;
        return "$" + len + "\r\n" + s + "\r\n";
    }

    public static String bulkArray(List<String> items) {
        if (items == null) return NULL_ARRAY;
        StringBuilder sb = new StringBuilder("*").append(items.size()).append("\r\n");
        for (String item : items) sb.append(bulk(item));
        return sb.toString();
    }

    /** An array whose elements are already-encoded replies (used by EXEC). */
    public static String rawArray(List<String> encoded) {
        StringBuilder sb = new StringBuilder("*").append(encoded.size()).append("\r\n");
        for (String e : encoded) sb.append(e);
        return sb.toString();
    }
}
