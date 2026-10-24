package rtfl.io;

import rtfl.common.EnrichedTx;
import rtfl.common.TxEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class OracleTxArchiver implements AutoCloseable {
    private static final long FLUSH_INTERVAL_S = 5;
    private static final int BATCH_SIZE = 5_000;

    private static final String INSERT_TX = "INSERT INTO tx_events (transaction_id, user_id, amount, currency,"
            + " merchant_id, device_id, timestamp, send_ts_ms, expected_profile_version)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    // Sink enrich là at-least-once -> bỏ qua bản ghi trùng thay vì làm hỏng toàn bộ batch
    private static final String INSERT_ENRICHED = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(enriched_tx(transaction_id)) */"
            + " INTO enriched_tx (transaction_id, user_id, amount, currency, merchant_id, device_id, timestamp,"
            + " kyc_level, age, monthly_income, card_limit, avg_amount_90d, avg_amount_120d, avg_amount_180d,"
            + " risk_score, cic_group, historical_risk_score, has_transferred_before, account_age_days,"
            + " enriched_profile_version, profile_found, lookup_status, send_ts_ms, expected_profile_version)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final Connection conn;
    private final Queue<TxEvent> txQueue = new ConcurrentLinkedQueue<>();
    private final Queue<EnrichedTx> enrichedQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Thread reader;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean running = true;
    private long txTotal, enrichedTotal;

    public OracleTxArchiver(Args.Endpoints ep, String outSource) throws SQLException {
        conn = DriverManager.getConnection(ep.oracleUrl(), ep.oracleUser(), ep.oraclePassword());
        conn.setAutoCommit(false);
        // Mở source trước khi bắt đầu gửi tx, để chốt vị trí đọc
        EnrichedSource source = EnrichedSource.create(outSource, ep);
        reader = new Thread(() -> readEnriched(source), "enriched-reader");
        reader.setDaemon(true);
        reader.start();
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                flush();
            } catch (Exception e) {
                System.err.println("[oracle] flush failed: " + e.getMessage());
            }
        }, FLUSH_INTERVAL_S, FLUSH_INTERVAL_S, TimeUnit.SECONDS);
    }

    /** Gọi từ thread sinh tx: chỉ đẩy vào hàng đợi. */
    public void addTx(TxEvent tx) {
        txQueue.add(tx);
    }

    // KafkaConsumer không thread-safe nên poll và close đều ở thread này
    private void readEnriched(EnrichedSource source) {
        try (source) {
            while (running) {
                enrichedQueue.addAll(source.poll(Duration.ofMillis(200)));
            }
        } catch (Exception e) {
            System.err.println("[oracle] enriched reader stopped: " + e.getMessage());
        }
    }

    private synchronized void flush() throws SQLException {
        List<TxEvent> txs = drain(txQueue);
        List<EnrichedTx> enriched = drain(enrichedQueue);
        if (txs.isEmpty() && enriched.isEmpty()) {
            return;
        }
        try {
            insertTx(txs);
            insertEnriched(enriched);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
        txTotal += txs.size();
        enrichedTotal += enriched.size();
        System.out.printf("[oracle] %s +%d tx, +%d enriched (total tx=%d enriched=%d)%n",
                Instant.now(), txs.size(), enriched.size(), txTotal, enrichedTotal);
    }

    private static <T> List<T> drain(Queue<T> queue) {
        List<T> out = new ArrayList<>();
        T item;
        while ((item = queue.poll()) != null) {
            out.add(item);
        }
        return out;
    }

    private void insertTx(List<TxEvent> txs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_TX)) {
            int n = 0;
            for (TxEvent t : txs) {
                ps.setString(1, t.transactionId);
                ps.setString(2, t.userId);
                ps.setLong(3, t.amount);
                ps.setString(4, t.currency);
                ps.setString(5, t.merchantId);
                ps.setString(6, t.deviceId);
                ps.setString(7, t.timestamp);
                ps.setLong(8, t.sendTsMs);
                ps.setLong(9, t.expectedProfileVersion);
                ps.addBatch();
                if (++n % BATCH_SIZE == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertEnriched(List<EnrichedTx> rows) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_ENRICHED)) {
            int n = 0;
            for (EnrichedTx e : rows) {
                ps.setString(1, e.transactionId);
                ps.setString(2, e.userId);
                ps.setLong(3, e.amount);
                ps.setString(4, e.currency);
                ps.setString(5, e.merchantId);
                ps.setString(6, e.deviceId);
                ps.setString(7, e.timestamp);
                setInt(ps, 8, e.kycLevel);
                setInt(ps, 9, e.age);
                setLong(ps, 10, e.monthlyIncome);
                setLong(ps, 11, e.cardLimit);
                setLong(ps, 12, e.avgAmount90d);
                setLong(ps, 13, e.avgAmount120d);
                setLong(ps, 14, e.avgAmount180d);
                setDouble(ps, 15, e.riskScore);
                setInt(ps, 16, e.cicGroup);
                setDouble(ps, 17, e.historicalRiskScore);
                setInt(ps, 18, e.hasTransferredBefore == null ? null : (e.hasTransferredBefore ? 1 : 0));
                setInt(ps, 19, e.accountAgeDays);
                setLong(ps, 20, e.enrichedProfileVersion);
                ps.setInt(21, e.profileFound ? 1 : 0);
                ps.setString(22, e.lookupStatus);
                ps.setLong(23, e.sendTsMs);
                ps.setLong(24, e.expectedProfileVersion);
                ps.addBatch();
                if (++n % BATCH_SIZE == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private static void setInt(PreparedStatement ps, int i, Integer v) throws SQLException {
        if (v == null) ps.setNull(i, Types.INTEGER); else ps.setInt(i, v);
    }

    private static void setLong(PreparedStatement ps, int i, Long v) throws SQLException {
        if (v == null) ps.setNull(i, Types.BIGINT); else ps.setLong(i, v);
    }

    private static void setDouble(PreparedStatement ps, int i, Double v) throws SQLException {
        if (v == null) ps.setNull(i, Types.DOUBLE); else ps.setDouble(i, v);
    }

    /** Ghi nốt phần còn trong hàng đợi. Gọi được nhiều lần (try-with-resources + shutdown hook khi Ctrl+C). */
    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // shutdown (không interrupt) để lần flush đang chạy, nếu có, ghi xong
        scheduler.shutdown();
        scheduler.awaitTermination(30, TimeUnit.SECONDS);
        // Chờ thêm chút cho output enrich của các tx cuối kịp tới
        Thread.sleep(1_000);
        running = false;
        reader.join(2_000);
        flush();
        conn.close();
    }
}
