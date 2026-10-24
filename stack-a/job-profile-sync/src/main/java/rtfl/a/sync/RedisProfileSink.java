package rtfl.a.sync;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// SinkFunction (API cũ) bị deprecated nhưng vẫn chạy trên Flink 1.20; giữ để flush theo checkpoint đơn giản
@SuppressWarnings("deprecation")
public class RedisProfileSink extends RichSinkFunction<ProfileChange> implements CheckpointedFunction {

    private static final String UPSERT_LUA =
            "local cur = redis.call('HGET', KEYS[1], 'profile_version')\n"
            + "if cur and tonumber(cur) > tonumber(ARGV[1]) then return 0 end\n"
            + "redis.call('HSET', KEYS[1], unpack(ARGV, 2))\n"
            + "return 1";
    private static final String DELETE_LUA =
            "local cur = redis.call('HGET', KEYS[1], 'profile_version')\n"
            + "if cur and tonumber(cur) > tonumber(ARGV[1]) then return 0 end\n"
            + "redis.call('DEL', KEYS[1])\n"
            + "return 1";

    private final String redisUri;
    private final int batchSize;
    private final long flushIntervalMs;

    private transient RedisClient client;
    private transient StatefulRedisConnection<String, String> conn;
    private transient RedisAsyncCommands<String, String> async;
    private transient String upsertSha;
    private transient String deleteSha;
    private transient List<ProfileChange> buffer;
    private transient ScheduledExecutorService scheduler;
    private transient AtomicReference<Exception> asyncError;

    public RedisProfileSink(String redisUri, int batchSize, long flushIntervalMs) {
        this.redisUri = redisUri;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
        client     = RedisClient.create(redisUri);
        conn       = client.connect();
        async      = conn.async();
        upsertSha  = conn.sync().scriptLoad(UPSERT_LUA);
        deleteSha  = conn.sync().scriptLoad(DELETE_LUA);
        buffer     = new ArrayList<>(batchSize);
        asyncError = new AtomicReference<>();
        scheduler  = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                flush();
            } catch (Exception e) {
                asyncError.compareAndSet(null, e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(ProfileChange change, Context context) throws Exception {
        Exception e = asyncError.get();
        if (e != null) {
            throw e;
        }
        boolean full;
        synchronized (this) {
            buffer.add(change);
            full = buffer.size() >= batchSize;
        }
        if (full) {
            flush();
        }
    }

    private synchronized void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        List<RedisFuture<?>> futures = new ArrayList<>(buffer.size());
        conn.setAutoFlushCommands(false);
        try {
            for (ProfileChange c : buffer) {
                String[] keys = {"user:" + c.userId};
                if (c.delete) {
                    futures.add(
                        async.evalsha(
                            deleteSha, 
                            ScriptOutputType.INTEGER, 
                            keys, 
                            Long.toString(c.version)
                        )
                    );
                } else {
                    String[] args = new String[1 + c.fields.size() * 2];
                    args[0] = Long.toString(c.version);
                    int i = 1;
                    for (Map.Entry<String, String> f : c.fields.entrySet()) {
                        args[i++] = f.getKey();
                        args[i++] = f.getValue();
                    }
                    futures.add(
                        async.evalsha(
                            upsertSha, 
                            ScriptOutputType.INTEGER, 
                            keys, 
                            args
                        )
                    );
                }
            }
            conn.flushCommands();
        } finally {
            conn.setAutoFlushCommands(true);
        }
        if (!LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]))) {
            throw new IllegalStateException("Redis batch write timed out");
        }
        buffer.clear();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (conn != null) {
            flush();
            conn.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}
