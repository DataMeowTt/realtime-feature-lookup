package rtfl.io;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import rtfl.common.Json;
import rtfl.common.TxEvent;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public interface TxSink extends AutoCloseable {

    void send(TxEvent tx);
    long failures();
    void flush();

    static TxSink create(String target, Args.Endpoints ep) {
        switch (target) {
            case "kafka": return new KafkaTxSink(ep.kafka());
            case "fluss": return new FlussTxSink(ep);
            default: throw new IllegalArgumentException("--target must be kafka|fluss, got " + target);
        }
    }

    final class KafkaTxSink implements TxSink {
        private final KafkaProducer<String, String> producer;
        private final AtomicLong failures = new AtomicLong();

        KafkaTxSink(String bootstrap) {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("key.serializer", StringSerializer.class.getName());
            p.put("value.serializer", StringSerializer.class.getName());
            p.put("linger.ms", "1");
            p.put("acks", "1");
            producer = new KafkaProducer<>(p);
        }

        @Override
        public void send(TxEvent tx) {
            producer.send(new ProducerRecord<>("tx_events", tx.userId, Json.write(tx)), (m, e) -> {
                if (e != null) {
                    failures.incrementAndGet();
                }
            });
        }

        @Override
        public long failures() {
            return failures.get();
        }

        @Override
        public void flush() {
            producer.flush();
        }

        @Override
        public void close() {
            producer.close();
        }
    }

    final class FlussTxSink implements TxSink {
        private final Connection conn;
        private final Table table;
        private final AppendWriter writer;
        private final AtomicLong failures = new AtomicLong();

        FlussTxSink(Args.Endpoints ep) {
            conn = FlussTables.connect(ep);
            table = conn.getTable(FlussTables.TX_EVENTS);
            writer = table.newAppend().createWriter();
        }

        @Override
        public void send(TxEvent tx) {
            writer.append(FlussTables.txRow(tx)).whenComplete((r, e) -> {
                if (e != null) {
                    failures.incrementAndGet();
                }
            });
        }

        @Override
        public long failures() {
            return failures.get();
        }

        @Override
        public void flush() {
            writer.flush();
        }

        @Override
        public void close() throws Exception {
            writer.flush();
            table.close();
            conn.close();
        }
    }
}
