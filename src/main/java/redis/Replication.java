package redis;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Replication state for one server, which is either a master or a replica.
 *
 * Master side: every write command is copied to each connected replica as a
 * RESP array (the "replication stream"). {@link #masterOffset} counts the
 * bytes sent on that stream. Replicas report how far they've got with
 * REPLCONF ACK, which is how WAIT knows when a write has reached N replicas.
 *
 * Replica side: {@link ReplicaClient} keeps the link to the master and
 * records its progress in the replica* fields here, for INFO.
 *
 * Methods marked "caller holds the lock" must run under the
 * CommandProcessor's global lock. That keeps stream order identical to
 * execution order.
 */
public final class Replication {
    /** A replica whose send queue grows past this many commands is disconnected, like Redis's output buffer limit. */
    static final int MAX_PENDING = 100_000;

    final String replId = randomId();
    long masterOffset; // caller holds the lock
    final List<ReplicaLink> replicas = new CopyOnWriteArrayList<>();
    private final Object ackMonitor = new Object();

    // Replica role (masterHost == null means we are a master)
    volatile String masterHost;
    volatile int masterPort;
    volatile boolean linkUp;
    volatile String masterReplId = "?";
    volatile long replicaOffset;

    boolean isReplica() {
        return masterHost != null;
    }

    boolean hasReplicas() {
        return !replicas.isEmpty();
    }

    /** Appends a command to the replication stream. Caller holds the lock. */
    void propagate(List<String> args) {
        if (replicas.isEmpty()) return;
        byte[] bytes = Resp.bulkArray(args).getBytes(StandardCharsets.UTF_8);
        masterOffset += bytes.length;
        for (ReplicaLink r : replicas) r.send(bytes);
    }

    /**
     * Handles PSYNC: registers a replica and starts a full resync from the
     * snapshot. Caller holds the lock, so no write can land between the
     * snapshot and the point where the replica starts receiving the stream.
     */
    ReplicaLink register(Socket socket, OutputStream out, int listeningPort, List<Rdb.Item> snapshot) {
        ReplicaLink link = new ReplicaLink(this, socket, out, listeningPort, masterOffset);
        replicas.add(link);
        link.start(replId, masterOffset, snapshot);
        return link;
    }

    void remove(ReplicaLink link) {
        if (replicas.remove(link)) System.out.println("Replica " + link.describe() + " disconnected");
        link.close();
    }

    void ack(ReplicaLink link, long offset) {
        link.ackOffset = offset;
        synchronized (ackMonitor) {
            ackMonitor.notifyAll();
        }
    }

    int countAcked(long target) {
        int n = 0;
        for (ReplicaLink r : replicas) if (r.ackOffset >= target) n++;
        return n;
    }

    /** Blocks until numReplicas have acknowledged target, or timeoutMs passes (0 = forever). */
    int awaitAcks(long target, int numReplicas, long timeoutMs) throws InterruptedException {
        long deadline = timeoutMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
        synchronized (ackMonitor) {
            while (countAcked(target) < numReplicas) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) break;
                ackMonitor.wait(Math.min(left, 1000)); // re-check periodically: replicas may disconnect
            }
        }
        return countAcked(target);
    }

    /** The "# Replication" section of INFO. Reads masterOffset racily, which is fine for INFO. */
    String info() {
        StringBuilder sb = new StringBuilder("# Replication\r\n");
        if (isReplica()) {
            sb.append("role:slave\r\n")
                    .append("master_host:").append(masterHost).append("\r\n")
                    .append("master_port:").append(masterPort).append("\r\n")
                    .append("master_link_status:").append(linkUp ? "up" : "down").append("\r\n")
                    .append("slave_repl_offset:").append(replicaOffset).append("\r\n")
                    .append("master_replid:").append(masterReplId).append("\r\n")
                    .append("master_repl_offset:").append(replicaOffset).append("\r\n");
        } else {
            sb.append("role:master\r\n").append("connected_slaves:").append(replicas.size()).append("\r\n");
            int i = 0;
            for (ReplicaLink r : replicas) {
                sb.append("slave").append(i++).append(":ip=").append(r.ip).append(",port=").append(r.listeningPort)
                        .append(",state=online,offset=").append(r.ackOffset).append("\r\n");
            }
            sb.append("master_replid:").append(replId).append("\r\n")
                    .append("master_repl_offset:").append(masterOffset).append("\r\n");
        }
        return sb.toString();
    }

    private static String randomId() {
        byte[] b = new byte[20];
        new SecureRandom().nextBytes(b);
        return HexFormat.of().formatHex(b); // 40 hex chars, like Redis
    }

    /** Master's view of one connected replica. Owns the socket's output stream after PSYNC. */
    static final class ReplicaLink {
        private final Replication repl;
        private final Socket socket;
        private final OutputStream out;
        private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
        final String ip;
        final int listeningPort;
        volatile long ackOffset;
        private Thread writer;

        ReplicaLink(Replication repl, Socket socket, OutputStream out, int listeningPort, long syncOffset) {
            this.repl = repl;
            this.socket = socket;
            this.out = out;
            this.ip = socket.getInetAddress().getHostAddress();
            this.listeningPort = listeningPort;
            this.ackOffset = syncOffset; // it has everything up to the snapshot
        }

        /**
         * Sends "+FULLRESYNC <replid> <offset>", then the snapshot as "$<len>\r\n<rdb bytes>"
         * (no trailing CRLF), then streams queued commands. The snapshot is serialized here,
         * off the global lock.
         */
        void start(String replId, long offset, List<Rdb.Item> snapshot) {
            writer = Thread.ofVirtual().name("replica-" + describe()).start(() -> {
                try {
                    byte[] rdb = Rdb.write(snapshot);
                    out.write(("+FULLRESYNC " + replId + " " + offset + "\r\n$" + rdb.length + "\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
                    out.write(rdb);
                    out.flush();
                    System.out.println("Replica " + describe() + " synced (" + snapshot.size() + " keys, "
                            + rdb.length + " bytes)");
                    while (true) {
                        byte[] b = queue.take();
                        out.write(b);
                        while ((b = queue.poll()) != null) out.write(b); // batch whatever else is waiting
                        out.flush();
                    }
                } catch (IOException | InterruptedException e) {
                    repl.remove(this);
                }
            });
        }

        void send(byte[] bytes) {
            if (queue.size() >= MAX_PENDING) {
                System.err.println("Replica " + describe() + " is too far behind, disconnecting");
                repl.remove(this);
                return;
            }
            queue.add(bytes);
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            if (writer != null) writer.interrupt();
        }

        String describe() {
            return ip + ":" + listeningPort;
        }
    }
}
