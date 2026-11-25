package redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** TCP server: accepts connections and gives each client its own virtual thread. */
public final class RedisServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final CommandProcessor processor;
    private final ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService expiry = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "active-expiry");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running = true;

    public RedisServer(int port, Map<String, String> config) throws IOException {
        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new InetSocketAddress(port));
        this.processor = new CommandProcessor(new Store(), config);
        try {
            int loaded = processor.loadFromDisk();
            if (loaded >= 0) System.out.println("Loaded " + loaded + " keys from " + processor.rdbFile());
        } catch (IOException e) {
            // Like Redis: refuse to start rather than run with (and later overwrite) a bad dump.
            serverSocket.close();
            throw new IOException("failed to load " + processor.rdbFile() + ": " + e.getMessage(), e);
        }

        // --replicaof "<host> <port>": become a read-only copy of that master.
        String replicaOf = config.get("replicaof");
        if (replicaOf != null && !replicaOf.isBlank()) {
            String[] hp = replicaOf.trim().split("\\s+");
            if (hp.length != 2) throw new IOException("replicaof must be \"<host> <port>\", got: " + replicaOf);
            replicaClient = new ReplicaClient(hp[0], Integer.parseInt(hp[1]), port(), processor);
        }
    }

    private ReplicaClient replicaClient;

    public int port() {
        return serverSocket.getLocalPort();
    }

    public CommandProcessor processor() {
        return processor;
    }

    /** Blocks, accepting clients until {@link #close()} is called. */
    public void serve() {
        expiry.scheduleAtFixedRate(processor::sweepExpired, 100, 100, TimeUnit.MILLISECONDS);
        expiry.scheduleAtFixedRate(processor::checkSavePoints, 1, 1, TimeUnit.SECONDS);
        if (replicaClient != null) Thread.ofVirtual().name("replica-link").start(replicaClient);
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                clients.submit(() -> handleClient(socket));
            } catch (IOException e) {
                if (running) System.err.println("accept failed: " + e.getMessage());
            }
        }
    }

    private void handleClient(Socket socket) {
        CommandProcessor.Session session = new CommandProcessor.Session();
        try (socket) {
            socket.setTcpNoDelay(true);
            RespReader reader = new RespReader(new BufferedInputStream(socket.getInputStream()));
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());

            List<String> cmd;
            while ((cmd = reader.readCommand()) != null) {
                if (!cmd.isEmpty() && cmd.get(0).equalsIgnoreCase("QUIT")) {
                    out.write(Resp.OK.getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    return;
                }
                if (!cmd.isEmpty() && cmd.get(0).equalsIgnoreCase("PSYNC")
                        && session.queued == null && session.replicaLink == null) {
                    // This connection is now a replica. Its output belongs to the replication stream;
                    // we keep reading here only for REPLCONF ACK.
                    out.flush();
                    processor.psync(socket, out, session);
                    continue;
                }
                String reply = processor.handle(cmd, session);
                if (session.replicaLink != null) continue; // never interleave replies with the stream
                out.write(reply.getBytes(StandardCharsets.UTF_8));
                // Pipelining: only flush once the client has nothing more queued.
                if (socket.getInputStream().available() == 0) out.flush();
            }
        } catch (RespReader.ProtocolException e) {
            System.err.println("protocol error from " + socket.getRemoteSocketAddress() + ": " + e.getMessage());
        } catch (IOException ignored) {
            // client went away
        } finally {
            processor.replicaDisconnected(session);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();
        if (replicaClient != null) replicaClient.close();
        for (Replication.ReplicaLink link : processor.replication().replicas) processor.replication().remove(link);
        expiry.shutdownNow();
        clients.shutdownNow();
    }
}
