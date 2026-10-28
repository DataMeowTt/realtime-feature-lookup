package rtfl.io;

import java.util.HashMap;
import java.util.Map;

public final class Args {
    private final Map<String, String> values = new HashMap<>();

    public Args(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + args[i]);
            }
            String key = args[i].substring(2);
            String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
            values.put(key, value);
        }
    }

    public String get(String key, String def) {
        return values.getOrDefault(key, def);
    }

    public int getInt(String key, int def) {
        return values.containsKey(key) ? Integer.parseInt(values.get(key)) : def;
    }

    public long getLong(String key, long def) {
        return values.containsKey(key) ? Long.parseLong(values.get(key)) : def;
    }

    public double getDouble(String key, double def) {
        return values.containsKey(key) ? Double.parseDouble(values.get(key)) : def;
    }

    public Endpoints endpoints() {
        return new Endpoints(
                get("kafka", "kafka:9092"),
                get("fluss", "coordinator-server:9123"),
                get("redis", "redis://redis-1:6379"),
                get("oracle-url", "jdbc:oracle:thin:@//oracle:1521/XEPDB1"),
                get("oracle-user", "bank"),
                get("oracle-password", "bank"),
                get("fluss-batch-timeout", "5ms"),
                get("fluss-fetch-wait", "10ms"));
    }

    public record Endpoints(String kafka, String fluss, String redis,
                            String oracleUrl, String oracleUser, String oraclePassword,
                            String flussBatchTimeout, String flussFetchWait) {}
}
