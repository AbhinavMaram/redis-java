package redis;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Main {
    public static void main(String[] args) throws Exception {
        int port = 6379;
        Map<String, String> config = new LinkedHashMap<>();
        config.put("dir", System.getProperty("user.dir"));
        config.put("dbfilename", "dump.rdb");
        config.put("save", "3600 1 300 100 60 10000"); // Redis's defaults

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--dir" -> config.put("dir", args[++i]);
                case "--dbfilename" -> config.put("dbfilename", args[++i]);
                case "--save" -> config.put("save", args[++i]); // e.g. "60 1", or "" to disable
                case "--replicaof" -> config.put("replicaof", args[++i]); // "<host> <port>"
                default -> {
                    System.err.println("unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        }
        config.put("port", Integer.toString(port));
        CommandProcessor.parseSavePoints(config.get("save")); // fail fast on a bad --save

        RedisServer server;
        try {
            server = new RedisServer(port, config);
        } catch (java.io.IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        CommandProcessor processor = server.processor();
        processor.setShutdownAction(() -> System.exit(0));
        Runtime.getRuntime().addShutdownHook(new Thread(processor::saveOnExit, "save-on-exit"));

        System.out.println("redis-java listening on port " + server.port());
        server.serve();
    }
}
