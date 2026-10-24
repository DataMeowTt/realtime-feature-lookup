package rtfl.tests;

import rtfl.common.EnrichedTx;
import rtfl.io.EnrichedSource;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Đọc enriched_tx ở thread nền, giữ bản ghi đầu tiên của mỗi transaction_id. */
final class OutputCollector implements AutoCloseable {
    private final EnrichedSource source;
    private final Map<String, EnrichedTx> byTxId = new ConcurrentHashMap<>();
    private final Thread thread;
    private volatile boolean running = true;

    OutputCollector(EnrichedSource source) {
        this.source = source;
        thread = new Thread(() -> {
            while (running) {
                for (EnrichedTx e : source.poll(Duration.ofMillis(100))) {
                    byTxId.putIfAbsent(e.transactionId, e);
                }
            }
        }, "output-collector");
        thread.setDaemon(true);
        thread.start();
    }

    /** Chờ đủ các tx id; trả map (có thể thiếu nếu quá hạn). */
    Map<String, EnrichedTx> await(Collection<String> txIds, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && !byTxId.keySet().containsAll(txIds)) {
            Thread.sleep(50);
        }
        return byTxId;
    }

    @Override
    public void close() throws Exception {
        running = false;
        thread.join(2000);
        source.close();
    }
}
