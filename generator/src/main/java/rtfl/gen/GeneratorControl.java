package rtfl.gen;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class GeneratorControl implements AutoCloseable {
    private final Path file;
    private final int users;
    private final Random random;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile UserSampler sampler;
    private String lastContent = "";

    GeneratorControl(Path file, int users, Random random) {
        this.file = file;
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
                int hotUsers = Integer.parseInt(p.getProperty("hot-users", "20").trim());
                double share = Double.parseDouble(p.getProperty("hot-share", "0.8").trim());
                next = UserSampler.hotUsers(users, hotUsers, share, random);
            } else {
                next = UserSampler.uniform(users, random);
            }
            sampler = next;
            System.out.printf("[control] %s switched to %s%n", Instant.now(), next);
        } catch (Exception e) {
            System.err.printf("[control] %s ignored invalid config: %s (keeping %s)%n", Instant.now(), e, sampler);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
