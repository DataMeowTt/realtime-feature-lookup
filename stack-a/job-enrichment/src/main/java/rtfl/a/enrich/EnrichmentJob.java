package rtfl.a.enrich;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rtfl.common.Json;
import rtfl.common.TxEvent;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class EnrichmentJob {

    public static void main(String[] args) throws Exception {
        ParameterTool p  = ParameterTool.fromArgs(args);
        String bootstrap = p.get("kafka", "kafka:9092");
        String redis     = p.get("redis", "redis://redis-1:6379");
        long timeoutMs   = p.getLong("timeout-ms", 500);
        int capacity     = p.getInt("capacity", 500);
        boolean ordered  = p.getBoolean("ordered", false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("tx_events")
                .setGroupId("enrich-a")
                // Chỉ xử lý tx mới, không replay
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<TxEvent> tx = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tx_events")
                .map(s -> Json.read(s, TxEvent.class)).name("parse-tx");

        RedisLookupFunction lookup = new RedisLookupFunction(redis);
        SingleOutputStreamOperator<String> enriched = ordered
                ? AsyncDataStream.orderedWait(tx, lookup, timeoutMs, TimeUnit.MILLISECONDS, capacity)
                : AsyncDataStream.unorderedWait(tx, lookup, timeoutMs, TimeUnit.MILLISECONDS, capacity);

        Properties producer = new Properties();
        producer.setProperty("linger.ms", p.get("linger-ms", "0"));

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(producer)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("enriched_tx")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        enriched.name(ordered ? "redis-lookup-ordered" : "redis-lookup-unordered").sinkTo(sink).name("enriched_tx");

        env.execute("A2 enrichment (tx_events -> Redis lookup -> enriched_tx)"
                + " timeout=" + timeoutMs + "ms capacity=" + capacity + " ordered=" + ordered);
    }
}
