package rtfl.gen;

import rtfl.io.Args;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class GeneratorControl implements AutoCloseable {
    private final Path file;
    private final Args.Endpoints ep;
    private final int users;
    private final Random random;
    private final Map<String, int[]> partitionCache = new HashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile UserSampler sampler;
    private String lastContent = "";

    GeneratorControl(Path file, Args.Endpoints ep, int users, Random random) {
        this.file = file;
        this.ep = ep;
        this.users = users;
        this.random = random;
        this.sampler = UserSampler.uniform(users, random);
        reload();
        System.out.printf("[control] watching %s, mode: %s%n", file, sampler);
        scheduler.scheduleWithFixedDelay(this::reload, 1, 1, TimeUnit.SECONDS);
    }

    UserSampler sampler() {
        return sampler;
    }

    private void reload() {
        try {
            String content = Files.exists(file) ? Files.readString(file) : "";
            if (content.equals(lastContent)) {
                return;
            }
            lastContent = content;
            Properties p = new Properties();
            try (Reader r = new java.io.StringReader(content)) {
                p.load(r);
            }
            UserSampler next;
            if ("hot".equals(p.getProperty("mode", "uniform").trim())) {
                String hotBy = p.getProperty("hot-by", "kafka").trim();
                double share = Double.parseDouble(p.getProperty("hot-share", "0.8").trim());
                int partition = Integer.parseInt(p.getProperty("hot-partition", "0").trim());
                next = UserSampler.hot(partitions(hotBy), partition, share, random, hotBy);
            } else {
                next = UserSampler.uniform(users, random);
            }
            sampler = next;
            System.out.printf("[control] %s switched to %s%n", Instant.now(), next);
        } catch (Exception e) {
            System.err.printf("[control] %s ignored invalid config: %s (keeping %s)%n", Instant.now(), e, sampler);
        }
    }

    private int[] partitions(String hotBy) throws Exception {
        int[] cached = partitionCache.get(hotBy);
        if (cached == null) {
            switch (hotBy) {
                case "kafka": cached = Partitioning.kafka(ep, users); break;
                case "fluss": cached = Partitioning.fluss(ep, users); break;
                default: throw new IllegalArgumentException("hot-by must be kafka|fluss, got " + hotBy);
            }
            partitionCache.put(hotBy, cached);
        }
        return cached;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
