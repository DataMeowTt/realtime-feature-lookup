package rtfl.io;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import rtfl.common.EnrichedTx;
import rtfl.common.Json;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface EnrichedSource extends AutoCloseable {

    List<EnrichedTx> poll(Duration timeout);

    static EnrichedSource create(String source, Args.Endpoints ep) {
        switch (source) {
            case "kafka": return new KafkaEnrichedSource(ep.kafka());
            case "fluss": return new FlussEnrichedSource(ep);
            default: throw new IllegalArgumentException("--source must be kafka|fluss, got " + source);
        }
    }

    final class KafkaEnrichedSource implements EnrichedSource {
        private final KafkaConsumer<String, String> consumer;

        KafkaEnrichedSource(String bootstrap) {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("key.deserializer", StringDeserializer.class.getName());
            p.put("value.deserializer", StringDeserializer.class.getName());
            p.put("enable.auto.commit", "false");
            p.put("fetch.max.wait.ms", "5");
            p.put("max.poll.records", "5000");
            consumer = new KafkaConsumer<>(p);
            List<TopicPartition> parts = consumer.partitionsFor("enriched_tx").stream()
                    .map((PartitionInfo pi) -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());
            consumer.assign(parts);
            consumer.seekToEnd(parts);
            parts.forEach(consumer::position); // chốt vị trí ngay bây giờ
        }

        @Override
        public List<EnrichedTx> poll(Duration timeout) {
            List<EnrichedTx> out = new ArrayList<>();
            for (ConsumerRecord<String, String> r : consumer.poll(timeout)) {
                out.add(Json.read(r.value(), EnrichedTx.class));
            }
            return out;
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

    final class FlussEnrichedSource implements EnrichedSource {
        private final Connection conn;
        private final Table table;
        private final LogScanner scanner;

        FlussEnrichedSource(Args.Endpoints ep) {
            conn = FlussTables.connect(ep);
            table = conn.getTable(FlussTables.ENRICHED_TX);
            int buckets = table.getTableInfo().getNumBuckets();
            List<Integer> ids = IntStream.range(0, buckets).boxed().collect(Collectors.toList());
            Map<Integer, Long> latest;
            try {
                latest = conn.getAdmin().listOffsets(FlussTables.ENRICHED_TX, ids, new OffsetSpec.LatestSpec()).all().get();
            } catch (Exception e) {
                throw new IllegalStateException("Cannot list offsets of enriched_tx", e);
            }
            scanner = table.newScan().createLogScanner();
            latest.forEach(scanner::subscribe);
        }

        @Override
        public List<EnrichedTx> poll(Duration timeout) {
            List<EnrichedTx> out = new ArrayList<>();
            for (ScanRecord r : scanner.poll(timeout)) {
                out.add(FlussTables.enriched(r.getRow()));
            }
            return out;
        }

        @Override
        public void close() throws Exception {
            scanner.close();
            table.close();
            conn.close();
        }
    }
}
