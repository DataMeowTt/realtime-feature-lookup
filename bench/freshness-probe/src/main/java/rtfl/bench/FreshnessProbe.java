package rtfl.bench;

import org.HdrHistogram.Histogram;
import rtfl.io.Args;
import rtfl.io.CsvOut;
import rtfl.io.OracleDb;
import rtfl.io.ProfileStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.locks.LockSupport;

public class FreshnessProbe {

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String store = a.get("store", "redis");
        String stack = a.get("stack", store);
        String user = a.get("user", "usr_probe");
        int count = a.getInt("count", 60);
        long intervalMs = a.getLong("interval-ms", 1000);
        long timeoutMs = a.getLong("timeout-ms", 60_000);
        long pollUs = a.getLong("poll-us", 500);
        String runId = a.get("run-id", Long.toString(System.currentTimeMillis(), 36));
        Path outDir = Path.of(a.get("out", "bench/results"));
        Files.createDirectories(outDir);

        Histogram hist = new Histogram(3_600_000L, 3);
        int timedOut = 0;
        Path samples = outDir.resolve(runId + "_freshness.csv");

        try (OracleDb db = new OracleDb(a.endpoints()); ProfileStore ps = ProfileStore.create(store, a.endpoints())) {
            long v0 = db.ensureUser(user);
            System.out.printf("freshness-probe store=%s user=%s, waiting until store has version %d...%n", store, user, v0);
            waitFor(ps, user, v0, 300_000, pollUs);

            Files.writeString(samples, "i,version,lag_ms\n");
            for (int i = 0; i < count; i++) {
                long loopStart = System.currentTimeMillis();
                long v = db.update(user, (i % 1000) / 1000.0);
                long committed = System.currentTimeMillis();
                long seen = waitFor(ps, user, v, timeoutMs, pollUs);
                if (seen < 0) {
                    timedOut++;
                    System.out.printf("#%d version=%d TIMEOUT%n", i, v);
                } else {
                    long lag = seen - committed;
                    hist.recordValue(Math.max(0, lag));
                    Files.writeString(samples, i + "," + v + "," + lag + "\n", StandardOpenOption.APPEND);
                    System.out.printf("#%d version=%d lag=%dms%n", i, v, lag);
                }
                long sleep = intervalMs - (System.currentTimeMillis() - loopStart);
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
        }

        String header = "run_id,stack,store,start,samples,timeouts,p50_ms,p95_ms,p99_ms,max_ms,mean_ms";
        String row = String.format(Locale.ROOT, "%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%.1f",
                runId, stack, store, Instant.now(), hist.getTotalCount(), timedOut,
                hist.getValueAtPercentile(50), hist.getValueAtPercentile(95), hist.getValueAtPercentile(99),
                hist.getMaxValue(), hist.getMean());
        CsvOut.append(outDir.resolve("freshness_summary.csv"), header, row);
        System.out.println(header);
        System.out.println(row);
        System.exit(0);
    }

    /** Trả thời điểm (ms) store có version >= v, hoặc -1 nếu quá hạn. */
    private static long waitFor(ProfileStore ps, String user, long v, long timeoutMs, long pollUs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (ps.version(user) >= v) {
                return System.currentTimeMillis();
            }
            LockSupport.parkNanos(pollUs * 1000);
        }
        return -1;
    }
}
