package rtfl.a.sync;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ProfileSyncJob {

    public static void main(String[] args) throws Exception {
        ParameterTool p  = ParameterTool.fromArgs(args);
        String bootstrap = p.get("kafka", "kafka:9092");
        String redis     = p.get("redis", "redis://redis:6379");
        int batchSize    = p.getInt("batch-size", 500);
        long flushMs     = p.getLong("flush-ms", 5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(p.get("topic", "cdc.users"))
                .setGroupId("profile-sync")
                // deploy.sh xoá group + FLUSHALL Redis -> mỗi lần deploy đọc lại cdc.users từ đầu;
                // khi failover, offset được khôi phục từ checkpoint
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "cdc.users")
                .flatMap((String msg, Collector<ProfileChange> out) -> {
                    ProfileChange c = ProfileChange.parse(msg);
                    if (c != null) {
                        out.collect(c);
                    }
                }).returns(ProfileChange.class).name("parse-debezium")
                // Cùng user luôn vào cùng subtask -> giữ thứ tự thay đổi theo key
                .keyBy(c -> c.userId)
                .addSink(new RedisProfileSink(redis, batchSize, flushMs)).name("redis-sink");

        env.execute("A1 profile-sync (cdc.users -> Redis)");
    }
}
