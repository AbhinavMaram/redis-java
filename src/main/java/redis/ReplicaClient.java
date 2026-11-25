package redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The replica's connection to its master. Runs the handshake, loads the
 * snapshot, then applies the replication stream. Reconnects (with a full
 * resync) if the link drops.
 *
 * Handshake:
 *   -> PING                              <- +PONG
 *   -> REPLCONF listening-port <port>    <- +OK
 *   -> REPLCONF capa psync2              <- +OK
 *   -> PSYNC ? -1                        <- +FULLRESYNC <replid> <offset>
 *                                        <- $<len>\r\n<RDB bytes>
 * After that the master sends write commands. The replica sends nothing back
 * except "REPLCONF ACK <offset>" in reply to "REPLCONF GETACK *".
 */
final class ReplicaClient implements Runnable {
    private static final long RETRY_MS = 1000;

    private final String host;
    private final int port;
    private final int myPort;
    private final CommandProcessor processor;
    private final Replication repl;
    private volatile boolean running = true;
    private volatile Socket socket;

    ReplicaClient(String host, int port, int myPort, CommandProcessor processor) {
        this.host = host;
        this.port = port;
        this.myPort = myPort;
        this.processor = processor;
        this.repl = processor.replication();
        repl.masterHost = host;
        repl.masterPort = port;
    }

    @Override
    public void run() {
        while (running) {
            try {
                syncAndStream();
            } catch (IOException e) {
                if (running) System.err.println("Replication link to " + host + ":" + port + " failed: " + e.getMessage());
            } finally {
                repl.linkUp = false;
            }
            if (!running) return;
            try {
                Thread.sleep(RETRY_MS);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void syncAndStream() throws IOException {
        try (Socket s = new Socket()) {
            socket = s;
            s.connect(new InetSocketAddress(host, port), 5000);
            s.setTcpNoDelay(true);
            CountingInputStream in = new CountingInputStream(new BufferedInputStream(s.getInputStream()));
            RespReader reader = new RespReader(in);
            OutputStream out = new BufferedOutputStream(s.getOutputStream());

            expect(reader, out, List.of("PING"), "+PONG");
            expect(reader, out, List.of("REPLCONF", "listening-port", Integer.toString(myPort)), "+OK");
            expect(reader, out, List.of("REPLCONF", "capa", "psync2"), "+OK");
            send(out, List.of("PSYNC", "?", "-1"));
            String reply = reader.readSimpleReply();
            String[] parts = reply.split(" ");
            if (!parts[0].equals("+FULLRESYNC") || parts.length != 3) {
                throw new IOException("unexpected PSYNC reply: " + reply);
            }
            byte[] rdb = reader.readRdbTransfer();
            int keys = processor.loadFromMaster(rdb);

            repl.masterReplId = parts[1];
            long offset = Long.parseLong(parts[2]);
            repl.replicaOffset = offset;
            repl.linkUp = true;
            System.out.println("Synced with master " + host + ":" + port + ": " + keys + " keys, offset " + offset);

            List<List<String>> tx = null; // commands between MULTI and EXEC
            while (running) {
                long before = in.count();
                List<String> cmd = reader.readCommand();
                if (cmd == null) throw new IOException("master closed the connection");
                long len = in.count() - before;
                String name = cmd.isEmpty() ? "" : cmd.get(0).toUpperCase(Locale.ROOT);

                if (name.equals("REPLCONF") && cmd.size() >= 2 && cmd.get(1).equalsIgnoreCase("GETACK")) {
                    // Report the offset *before* this GETACK, then count it.
                    send(out, List.of("REPLCONF", "ACK", Long.toString(offset)));
                } else if (name.equals("MULTI")) {
                    tx = new ArrayList<>();
                } else if (name.equals("EXEC") && tx != null) {
                    processor.applyFromMaster(tx);
                    tx = null;
                } else if (tx != null) {
                    tx.add(cmd);
                } else if (!name.isEmpty()) {
                    processor.applyFromMaster(List.of(cmd)); // includes PING: applied, reply dropped
                }
                offset += len;
                repl.replicaOffset = offset;
            }
        } finally {
            socket = null;
        }
    }

    private static void expect(RespReader reader, OutputStream out, List<String> cmd, String want) throws IOException {
        send(out, cmd);
        String reply = reader.readSimpleReply();
        if (!reply.equalsIgnoreCase(want)) {
            throw new IOException("master replied " + reply + " to " + String.join(" ", cmd) + " (expected " + want + ")");
        }
    }

    private static void send(OutputStream out, List<String> cmd) throws IOException {
        out.write(Resp.bulkArray(cmd).getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    void close() {
        running = false;
        Socket s = socket;
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }

    /** Counts bytes consumed, so the replica knows its exact offset in the stream. */
    static final class CountingInputStream extends FilterInputStream {
        private long count;

        CountingInputStream(InputStream in) {
            super(in);
        }

        long count() {
            return count;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) count++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) count += n;
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long s = super.skip(n);
            count += s;
            return s;
        }
    }
}
