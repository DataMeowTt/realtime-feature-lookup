package rtfl.tests;

import rtfl.common.EnrichedTx;
import rtfl.common.TxEvent;
import rtfl.io.Args;
import rtfl.io.CsvOut;
import rtfl.io.EnrichedSource;
import rtfl.io.OracleDb;
import rtfl.io.TxSink;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Kịch bản kiểm tra
 *  --mode correctness : E-S1, E-S2, E-S3, E-S5, E-S6 -> PASS/FAIL, exit code 1 nếu có FAIL
 *  --mode freshness   : E-S4, stale read rate theo từng Δ -> bench/results/es4.csv
 */
public class ScenarioRunner {

    private final Args.Endpoints ep;
    private final OracleDb db;
    private final TxSink sink;
    private final OutputCollector out;
    private final long cdcWaitMs;
    private final long outputTimeoutMs;
    private final String runId = Long.toString(System.currentTimeMillis(), 36);
    private long seq;
    private int failures;

    ScenarioRunner(Args a) throws Exception {
        ep = a.endpoints();
        db = new OracleDb(ep);
        sink = TxSink.create(a.get("tx-target", "kafka"), ep);
        out = new OutputCollector(EnrichedSource.create(a.get("out-source", "kafka"), ep));
        cdcWaitMs = a.getLong("cdc-wait-ms", 10_000);
        outputTimeoutMs = a.getLong("output-timeout-ms", 30_000);
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        ScenarioRunner r = new ScenarioRunner(a);
        String mode = a.get("mode", "correctness");
        try {
            if ("freshness".equals(mode)) {
                r.freshness(a.get("stack", "?"), a.get("deltas", "0,10,50,100,250,500,1000,2000,3000,5000"),
                        a.getInt("reps", 20), Path.of(a.get("out", "bench/results")));
            } else {
                r.correctness();
            }
        } finally {
            r.close();
        }
        System.exit(r.failures == 0 ? 0 : 1);
    }

    // ---------------------------------------------------------------- correctness

    void correctness() throws Exception {
        // E-S1: user tồn tại, không update -> cột khớp DB
        Map<String, String> txToUser = new LinkedHashMap<>();
        Map<String, Map<String, Object>> expected = new LinkedHashMap<>();
        for (int i = 11; i <= 30; i++) {
            String user = "usr_" + i;
            Map<String, Object> p = db.profile(user);
            TxEvent tx = send(user, p == null ? -1 : (Long) p.get("profile_version"));
            txToUser.put(tx.transactionId, user);
            expected.put(tx.transactionId, p);
        }
        Map<String, EnrichedTx> got = out.await(txToUser.keySet(), outputTimeoutMs);
        int ok = 0;
        for (var e : expected.entrySet()) {
            EnrichedTx r = got.get(e.getKey());
            if (r != null && matches(r, e.getValue())) ok++;
            else System.out.println("  mismatch " + e.getKey() + " db=" + e.getValue() + " out=" + describe(r));
        }
        check("E-S1 existing users match DB", ok == expected.size(), ok + "/" + expected.size());

        // E-S2: user không tồn tại
        TxEvent missing = send("usr_missing_" + runId, -1);
        EnrichedTx r2 = out.await(List.of(missing.transactionId), outputTimeoutMs).get(missing.transactionId);
        check("E-S2 unknown user -> profile_found=false, NULL columns",
                r2 != null && !r2.profileFound && r2.kycLevel == null && r2.enrichedProfileVersion == null, describe(r2));

        // E-S3: update risk_score, chờ, gửi tx -> mang giá trị mới
        double score = (System.currentTimeMillis() % 10_000) / 10_000.0;
        long v3 = db.update("usr_41", score);
        Thread.sleep(cdcWaitMs);
        TxEvent t3 = send("usr_41", v3);
        EnrichedTx r3 = out.await(List.of(t3.transactionId), outputTimeoutMs).get(t3.transactionId);
        check("E-S3 update visible after " + cdcWaitMs + "ms",
                r3 != null && Objects.equals(r3.riskScore, score) && Objects.equals(r3.enrichedProfileVersion, v3), describe(r3));

        // E-S5: DELETE user rồi gửi tx -> profile_found=false
        String tmp = "usr_es5_" + runId;
        db.insert(tmp);
        Thread.sleep(cdcWaitMs);
        db.delete(tmp);
        Thread.sleep(cdcWaitMs);
        TxEvent t5 = send(tmp, -1);
        EnrichedTx r5 = out.await(List.of(t5.transactionId), outputTimeoutMs).get(t5.transactionId);
        check("E-S5 deleted user -> profile_found=false", r5 != null && !r5.profileFound, describe(r5));

        // E-S6: nhiều update liên tiếp cùng user -> version không bao giờ đi lùi (theo thứ tự gửi)
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long v = db.update("usr_61", i / 10.0);
            ids.add(send("usr_61", v).transactionId);
            Thread.sleep(300);
        }
        Map<String, EnrichedTx> got6 = out.await(ids, outputTimeoutMs + cdcWaitMs);
        long prev = Long.MIN_VALUE;
        boolean monotonic = true;
        StringBuilder trace = new StringBuilder();
        for (String id : ids) {
            EnrichedTx r = got6.get(id);
            long v = r == null || r.enrichedProfileVersion == null ? -1 : r.enrichedProfileVersion;
            trace.append(v).append(' ');
            if (r == null || v < prev) monotonic = false;
            prev = Math.max(prev, v);
        }
        check("E-S6 versions never go backwards", monotonic, "versions in send order: " + trace.toString().trim());

        System.out.println(failures == 0 ? "ALL PASSED" : failures + " FAILED");
    }

    private static boolean matches(EnrichedTx r, Map<String, Object> p) {
        if (p == null) return !r.profileFound;
        return r.profileFound
                && Objects.equals(r.kycLevel, p.get("kyc_level"))
                && Objects.equals(r.age, p.get("age"))
                && Objects.equals(r.monthlyIncome, p.get("monthly_income"))
                && Objects.equals(r.cardLimit, p.get("card_limit"))
                && Objects.equals(r.avgAmount90d, p.get("avg_amount_90d"))
                && Objects.equals(r.avgAmount120d, p.get("avg_amount_120d"))
                && Objects.equals(r.avgAmount180d, p.get("avg_amount_180d"))
                && close(r.riskScore, (Double) p.get("risk_score"))
                && Objects.equals(r.cicGroup, p.get("cic_group"))
                && close(r.historicalRiskScore, (Double) p.get("historical_risk_score"))
                && Objects.equals(r.hasTransferredBefore, p.get("has_transferred_before"))
                && Objects.equals(r.accountAgeDays, p.get("account_age_days"))
                && Objects.equals(r.enrichedProfileVersion, p.get("profile_version"));
    }

    private static boolean close(Double a, double b) {
        return a != null && Math.abs(a - b) < 1e-9;
    }

    // ---------------------------------------------------------------- E-S4 freshness

    void freshness(String stack, String deltas, int reps, Path outDir) throws Exception {
        int userIdx = 2000;
        Map<Integer, List<String>> txByDelta = new LinkedHashMap<>();
        for (String d : deltas.split(",")) {
            int delta = Integer.parseInt(d.trim());
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < reps; i++) {
                String user = "usr_" + (userIdx++);
                long v = db.update(user, 0.99); // trả về sau khi commit
                if (delta > 0) Thread.sleep(delta);
                ids.add(send(user, v).transactionId);
                Thread.sleep(100);
            }
            txByDelta.put(delta, ids);
            System.out.printf("Δ=%dms: sent %d tx%n", delta, ids.size());
        }
        List<String> all = new ArrayList<>();
        txByDelta.values().forEach(all::addAll);
        Map<String, EnrichedTx> got = out.await(all, outputTimeoutMs);

        String header = "run_id,stack,start,delta_ms,reps,received,stale,stale_rate";
        for (var e : txByDelta.entrySet()) {
            int received = 0, stale = 0;
            for (String id : e.getValue()) {
                EnrichedTx r = got.get(id);
                if (r == null) continue;
                received++;
                if (r.isStale()) stale++;
            }
            double rate = received == 0 ? Double.NaN : (double) stale / received;
            String row = String.format(Locale.ROOT, "%s,%s,%s,%d,%d,%d,%d,%.4f",
                    runId, stack, Instant.now(), e.getKey(), reps, received, stale, rate);
            CsvOut.append(outDir.resolve("es4.csv"), header, row);
            System.out.printf("E-S4 %s Δ=%4dms stale=%d/%d (%.0f%%)%n", stack, e.getKey(), stale, received, rate * 100);
        }
    }

    // ---------------------------------------------------------------- helpers

    private TxEvent send(String user, long expectedVersion) {
        TxEvent tx = new TxEvent();
        tx.transactionId = "tx_test_" + runId + "_" + (seq++);
        tx.userId = user;
        tx.amount = 25_000_000;
        tx.currency = "VND";
        tx.merchantId = "mer_test";
        tx.deviceId = "dev_test";
        tx.timestamp = Instant.now().toString();
        tx.expectedProfileVersion = expectedVersion;
        tx.sendTsMs = System.currentTimeMillis();
        sink.send(tx);
        sink.flush();
        return tx;
    }

    private void check(String name, boolean pass, String detail) {
        System.out.printf("[%s] %s  (%s)%n", pass ? "PASS" : "FAIL", name, detail);
        if (!pass) failures++;
    }

    private static String describe(EnrichedTx r) {
        if (r == null) return "no output";
        return "found=" + r.profileFound + " version=" + r.enrichedProfileVersion + " risk_score=" + r.riskScore
                + (r.lookupStatus == null ? "" : " lookup=" + r.lookupStatus);
    }

    void close() throws Exception {
        out.close();
        sink.close();
        db.close();
    }
}
