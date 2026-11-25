package redis;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads client commands in the Redis Serialization Protocol (RESP2).
 *
 * Clients send commands as an array of bulk strings:
 *   *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
 * Redis also accepts "inline" commands (plain text lines, e.g. from telnet),
 * which are supported here as well.
 */
public final class RespReader {
    private final InputStream in;

    public RespReader(InputStream in) {
        this.in = in;
    }

    /** Returns the next command as a list of arguments, or null on clean EOF. */
    public List<String> readCommand() throws IOException {
        int first = in.read();
        if (first == -1) return null;

        if (first == '*') {
            int count = parseInt(readLine());
            List<String> args = new ArrayList<>(Math.max(count, 0));
            for (int i = 0; i < count; i++) {
                args.add(readBulkString());
            }
            return args;
        }

        // Inline command: the first byte is part of the line.
        String line = (char) first + readLine();
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return List.of();
        return List.of(trimmed.split("\\s+"));
    }

    /** Reads a one-line reply such as "+PONG" or "-ERR ...", returned with its type byte. */
    public String readSimpleReply() throws IOException {
        int type = in.read();
        if (type == -1) throw new EOFException();
        return (char) type + readLine();
    }

    /**
     * Reads the snapshot a master sends after +FULLRESYNC: "$<len>\r\n" then len bytes
     * of RDB, with no trailing CRLF. Real Redis may send bare "\n" keepalives first.
     */
    public byte[] readRdbTransfer() throws IOException {
        int b;
        while ((b = in.read()) == '\n') { }
        if (b == -1) throw new EOFException();
        if (b != '$') throw new ProtocolException("expected RDB transfer, got '" + (char) b + "'");
        int len = parseInt(readLine());
        if (len < 0) throw new ProtocolException("bad RDB transfer length " + len);
        byte[] data = in.readNBytes(len);
        if (data.length != len) throw new EOFException();
        return data;
    }

    private String readBulkString() throws IOException {
        int marker = in.read();
        if (marker == -1) throw new EOFException();
        if (marker != '$') throw new ProtocolException("expected '$', got '" + (char) marker + "'");
        int len = parseInt(readLine());
        if (len < 0) return null;
        byte[] data = in.readNBytes(len);
        if (data.length != len) throw new EOFException();
        if (in.read() != '\r' || in.read() != '\n') throw new ProtocolException("bulk string not terminated by CRLF");
        return new String(data, StandardCharsets.UTF_8);
    }

    /** Reads up to and consumes CRLF, returning the line without it. */
    private String readLine() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b == -1) throw new EOFException();
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') break;
                buf.write(b);
                if (next == -1) throw new EOFException();
                buf.write(next);
            } else if (b == '\n') {
                break; // tolerate bare LF for inline commands
            } else {
                buf.write(b);
            }
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    private static int parseInt(String s) throws ProtocolException {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new ProtocolException("invalid length: " + s);
        }
    }

    public static final class ProtocolException extends IOException {
        public ProtocolException(String msg) {
            super(msg);
        }
    }
}
