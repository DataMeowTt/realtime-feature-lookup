package rtfl.gen;

import rtfl.common.TxEvent;
import rtfl.io.Args;
import rtfl.io.OracleDb;
// import rtfl.io.OracleTxArchiver; 
import rtfl.io.TxSink;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class Generator {

    public static void main(String[] argv) throws Exception {
        Args a            = new Args(argv);
        Args.Endpoints ep = a.endpoints();
        String target     = a.get("target", "kafka");
        double rate       = a.getDouble("rate", 1000);
        long durationS    = a.getLong("duration", 60);
        int users         = a.getInt("users", 100_000);
        Path control      = Path.of(a.get("control", "bench/control/generator.conf"));
        double updateRate = a.getDouble("update-rate", 10);
        long seed         = a.getLong("seed", 42);
        String outSource  = a.get("out-source", "kafka");

        System.out.printf("generator target=%s rate=%.0f/s duration=%ds users=%d update-rate=%.1f/s seed=%d out-source=%s%n",
                target, rate, durationS, users, updateRate, seed, outSource);

        try (OracleDb db = new OracleDb(ep); OracleDb updater = new OracleDb(ep); TxSink sink = TxSink.create(target, ep)
             /* ; OracleTxArchiver archiver = new OracleTxArchiver(ep, outSource) */) {
            // Dừng bằng Ctrl+C vẫn ghi nốt hàng đợi vào Oracle
            // Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //     try {
            //         archiver.close();
            //     } catch (Exception e) {
            //         System.err.println("[oracle] final flush failed: " + e.getMessage());
            //     }
            // }));
            Map<String, Long> versions = new ConcurrentHashMap<>(db.loadVersions());
            System.out.printf("loaded %d profile versions from Oracle%n", versions.size());

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            AtomicLong updates = new AtomicLong();
            
            if (updateRate > 0) {
                Random ur = new Random(seed + 1);
                long periodNs = (long) (1_000_000_000L / updateRate);
                scheduler.scheduleAtFixedRate(() -> {
                    String userId = "usr_" + (ur.nextInt(users) + 1);
                    try {
                        long v = updater.update(userId, ur.nextDouble());
                        if (v > 0) {
                            versions.put(userId, v); // chỉ sau khi đã commit
                            updates.incrementAndGet();
                        }
                    } catch (Exception e) {
                        System.err.println("update failed: " + e.getMessage());
                    }
                }, periodNs, periodNs, TimeUnit.NANOSECONDS);
            }

            Random r             = new Random(seed);
            GeneratorControl ctl = new GeneratorControl(control, users, r);
            String runId         = Long.toString(System.currentTimeMillis(), 36);
            long start           = System.nanoTime();
            long end             = start + durationS * 1_000_000_000L;
            long sent            = 0;
            long nextReport      = start + 5_000_000_000L;
            long reportSent      = 0;

            while (true) {
                long now = System.nanoTime();
                if (now >= end) {
                    break;
                }

                long due = (long) ((now - start) / 1e9 * rate);
                UserSampler sampler = ctl.sampler();
                while (sent < due) {
                    int k = sampler.next();
                    String userId = "usr_" + k;
                    TxEvent tx = new TxEvent();
                    tx.transactionId = "tx_" + runId + "_" + sent;
                    tx.userId = userId;
                    tx.amount = 10_000 + (long) r.nextInt(50_000) * 1_000;
                    tx.currency = "VND";
                    tx.merchantId = "mer_" + (r.nextInt(1000) + 1);
                    tx.deviceId = r.nextInt(10) == 0 ? "dev_" + Integer.toHexString(r.nextInt()) : "dev_" + k;
                    tx.timestamp = Instant.now().toString();
                    tx.expectedProfileVersion = versions.getOrDefault(userId, -1L);
                    tx.sendTsMs = System.currentTimeMillis();
                    sink.send(tx);
                    // archiver.addTx(tx);
                    sent++;
                }
                if (now >= nextReport) {
                    System.out.printf("sent=%d (%.0f tx/s last 5s) updates=%d failures=%d%n",
                            sent, (sent - reportSent) / 5.0, updates.get(), sink.failures());
                    reportSent = sent;
                    nextReport += 5_000_000_000L;
                }
                LockSupport.parkNanos(200_000);
            }
            
            scheduler.shutdownNow();
            ctl.close();
            sink.flush();
            double elapsed = (System.nanoTime() - start) / 1e9;
            System.out.printf("done: sent=%d in %.1fs (%.0f tx/s), updates=%d, failures=%d%n",
                    sent, elapsed, sent / elapsed, updates.get(), sink.failures());
        }
        System.exit(0);
    }
}
