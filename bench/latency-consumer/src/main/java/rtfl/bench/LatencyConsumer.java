package rtfl.bench;

import org.HdrHistogram.Histogram;
import rtfl.common.EnrichedTx;
import rtfl.io.Args;
import rtfl.io.CsvOut;
import rtfl.io.EnrichedSource;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TreeMap;

public class LatencyConsumer {

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String source = a.get("source", "kafka");
        String stack = a.get("stack", source);
        String label = a.get("label", "adhoc");
        String targetRate = a.get("rate", "");
        String runId = a.get("run-id", Long.toString(System.currentTimeMillis(), 36));
        long warmupMs = a.getLong("warmup", 30) * 1000;
        long durationMs = a.getLong("duration", 300) * 1000;
        long drainMs = a.getLong("drain", 15) * 1000;
        Path outDir = Path.of(a.get("out", "bench/results"));

        long maxWaitMs = a.getLong("max-wait", 600) * 1000;
        long startMs = System.currentTimeMillis();
        // Cửa sổ đo neo vào send_ts_ms của bản ghi đầu tiên nhận được
        long windowStart = Long.MAX_VALUE;
        long windowEnd = Long.MAX_VALUE;

        Histogram hist = new Histogram(3_600_000L, 3);
        TreeMap<Long, Histogram> perSecond = new TreeMap<>();
        BitSet seenSeq = new BitSet();
        Set<String> seenOther = new HashSet<>();
        long count = 0, dups = 0, notFound = 0, timeouts = 0, stale = 0;

        Files.createDirectories(outDir);
        Path tsFile = outDir.resolve(runId + "_ts.csv");
        try (EnrichedSource src = EnrichedSource.create(source, a.endpoints());
             BufferedWriter ts = Files.newBufferedWriter(tsFile)) {
            ts.write("second,time,count,p50_ms,p99_ms,max_ms\n");
            System.out.printf("latency-consumer stack=%s source=%s run=%s warmup=%ds duration=%ds -> ready%n",
                    stack, source, runId, warmupMs / 1000, durationMs / 1000);

            while (windowEnd == Long.MAX_VALUE
                    ? System.currentTimeMillis() < startMs + maxWaitMs
                    : System.currentTimeMillis() < windowEnd + drainMs) {
                for (EnrichedTx e : src.poll(Duration.ofMillis(100))) {
                    long recv = System.currentTimeMillis();
                    if (windowStart == Long.MAX_VALUE) {
                        if (e.sendTsMs < startMs) {
                            continue; // bản ghi cũ còn sót lại
                        }
                        windowStart = e.sendTsMs + warmupMs;
                        windowEnd = windowStart + durationMs;
                    }
                    if (e.sendTsMs < windowStart || e.sendTsMs > windowEnd) {
                        continue;
                    }
                    if (!firstTime(e.transactionId, seenSeq, seenOther)) {
                        dups++;
                        continue;
                    }
                    long lat = Math.max(0, recv - e.sendTsMs);
                    hist.recordValue(Math.min(lat, 3_600_000L));
                    count++;
                    if (!e.profileFound) notFound++;
                    if ("TIMEOUT".equals(e.lookupStatus)) timeouts++;
                    if (e.isStale()) stale++;

                    perSecond.computeIfAbsent((e.sendTsMs - windowStart) / 1000, k -> new Histogram(3_600_000L, 2))
                            .recordValue(Math.min(lat, 3_600_000L));
                }
            }
            for (var sec : perSecond.entrySet()) {
                writeSecond(ts, sec.getKey(), windowStart + sec.getKey() * 1000, sec.getValue());
            }
        }

        double windowS = durationMs / 1000.0;
        String header = "run_id,stack,label,target_rate,start,window_s,count,throughput,dups,dup_rate,not_found,timeouts,"
                + "stale,stale_rate,p50_ms,p95_ms,p99_ms,p999_ms,max_ms,mean_ms";
        String row = String.format(Locale.ROOT, "%s,%s,%s,%s,%s,%.0f,%d,%.1f,%d,%.6f,%d,%d,%d,%.6f,%d,%d,%d,%d,%d,%.2f",
                runId, stack, label, targetRate, Instant.ofEpochMilli(startMs), windowS, count, count / windowS,
                dups, count == 0 ? 0 : (double) dups / count, notFound, timeouts,
                stale, count == 0 ? 0 : (double) stale / count,
                hist.getValueAtPercentile(50), hist.getValueAtPercentile(95), hist.getValueAtPercentile(99),
                hist.getValueAtPercentile(99.9), hist.getMaxValue(), hist.getMean());
        CsvOut.append(outDir.resolve("summary.csv"), header, row);
        System.out.println(header);
        System.out.println(row);
        System.exit(0);
    }

    private static boolean firstTime(String txId, BitSet seenSeq, Set<String> seenOther) {
        int i = txId.lastIndexOf('_');
        if (i > 0) {
            try {
                long seq = Long.parseLong(txId.substring(i + 1));
                if (seq < Integer.MAX_VALUE) {
                    boolean seen = seenSeq.get((int) seq);
                    seenSeq.set((int) seq);
                    return !seen;
                }
            } catch (NumberFormatException ignored) {
                // không phải id của generator
            }
        }
        return seenOther.add(txId);
    }

    private static void writeSecond(BufferedWriter w, long sec, long epochMs, Histogram h) throws Exception {
        w.write(String.format(Locale.ROOT, "%d,%s,%d,%d,%d,%d%n", sec, Instant.ofEpochMilli(epochMs), h.getTotalCount(),
                h.getValueAtPercentile(50), h.getValueAtPercentile(99), h.getMaxValue()));
    }
}
