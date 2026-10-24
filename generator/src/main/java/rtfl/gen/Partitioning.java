package rtfl.gen;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import rtfl.io.Args;
import rtfl.io.FlussTables;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

final class Partitioning {
    private Partitioning() {}

    static int[] kafka(Args.Endpoints ep, int users) {
        Properties p = new Properties();
        p.put("bootstrap.servers", ep.kafka());
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        int partitions;
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            partitions = c.partitionsFor("tx_events").size();
        }
        int[] out = new int[users + 1];
        for (int k = 1; k <= users; k++) {
            byte[] key = ("usr_" + k).getBytes(StandardCharsets.UTF_8);
            out[k] = Utils.toPositive(Utils.murmur2(key)) % partitions;
        }
        return out;
    }

    static int[] fluss(Args.Endpoints ep, int users) throws Exception {
        try (Connection conn = FlussTables.connect(ep); Table table = conn.getTable(FlussTables.USER_PROFILE)) {
            TableInfo info = table.getTableInfo();
            DataLakeFormat lake = info.getTableConfig().getDataLakeFormat().orElse(null);
            KeyEncoder encoder = KeyEncoder.ofBucketKeyEncoder(info.getRowType(), info.getBucketKeys(), lake);
            BucketingFunction bucketing = BucketingFunction.of(lake);
            int buckets = info.getNumBuckets();
            int fields = info.getRowType().getFieldCount();
            int[] out = new int[users + 1];
            for (int k = 1; k <= users; k++) {
                GenericRow row = new GenericRow(fields);
                row.setField(0, FlussTables.s("usr_" + k));
                out[k] = bucketing.bucketing(encoder.encodeKey(row), buckets);
            }
            return out;
        }
    }
}
