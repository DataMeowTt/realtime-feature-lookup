package rtfl.io;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

public interface ProfileStore extends AutoCloseable {

    /** profile_version đang thấy trong store, 
     * -1 nếu không có. */
    long version(String userId) throws Exception;

    static ProfileStore create(String store, Args.Endpoints ep) {
        switch (store) {
            case "redis": return new RedisStore(ep.redis());
            case "fluss": return new FlussStore(ep);
            default: throw new IllegalArgumentException("--store must be redis|fluss, got " + store);
        }
    }

    final class RedisStore implements ProfileStore {
        private final RedisClient client;
        private final StatefulRedisConnection<String, String> conn;

        RedisStore(String uri) {
            client = RedisClient.create(uri);
            conn = client.connect();
        }

        @Override
        public long version(String userId) {
            String v = conn.sync().hget("user:" + userId, "profile_version");
            return v == null ? -1 : Long.parseLong(v);
        }

        @Override
        public void close() {
            conn.close();
            client.shutdown();
        }
    }

    final class FlussStore implements ProfileStore {
        private final Connection conn;
        private final Table table;
        private final Lookuper lookuper;

        FlussStore(Args.Endpoints ep) {
            conn = FlussTables.connect(ep);
            table = conn.getTable(FlussTables.USER_PROFILE);
            lookuper = table.newLookup().createLookuper();
        }

        @Override
        public long version(String userId) throws Exception {
            InternalRow row = lookuper.lookup(GenericRow.of(FlussTables.s(userId))).get().getSingletonRow();
            return row == null ? -1 : row.getLong(FlussTables.PROFILE_VERSION_POS);
        }

        @Override
        public void close() throws Exception {
            table.close();
            conn.close();
        }
    }
}
