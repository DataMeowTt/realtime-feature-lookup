package rtfl.a.enrich;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import rtfl.common.EnrichedTx;
import rtfl.common.Json;
import rtfl.common.TxEvent;

import java.util.Collections;
import java.util.Map;

public class RedisLookupFunction extends RichAsyncFunction<TxEvent, String> {

    private final String redisUri;

    private transient RedisClient client;
    private transient StatefulRedisConnection<String, String> conn;
    private transient RedisAsyncCommands<String, String> async;
    private transient Counter timeouts;
    private transient Counter errors;
    private transient Histogram lookupLatencyUs;

    public RedisLookupFunction(String redisUri) {
        this.redisUri = redisUri;
    }

    @Override
    public void open(Configuration parameters) {
        client          = RedisClient.create(redisUri);
        conn            = client.connect();
        async           = conn.async();
        var group       = getRuntimeContext().getMetricGroup().addGroup("redis_lookup");
        timeouts        = group.counter("timeouts");
        errors          = group.counter("errors");
        lookupLatencyUs = group.histogram("latency_us", new DescriptiveStatisticsHistogram(10_000));
    }

    @Override
    public void asyncInvoke(TxEvent tx, ResultFuture<String> resultFuture) {
        long t0 = System.nanoTime();
        async.hgetall("user:" + tx.userId).whenComplete((profile, err) -> {
            lookupLatencyUs.update((System.nanoTime() - t0) / 1_000);
            if (err != null) {
                errors.inc();
                resultFuture.complete(Collections.singleton(Json.write(enrich(tx, null, "ERROR"))));
            } else {
                resultFuture.complete(Collections.singleton(Json.write(enrich(tx, profile, "OK"))));
            }
        });
    }

    @Override
    public void timeout(TxEvent tx, ResultFuture<String> resultFuture) {
        timeouts.inc();
        resultFuture.complete(Collections.singleton(Json.write(enrich(tx, null, "TIMEOUT"))));
    }

    static EnrichedTx enrich(TxEvent tx, Map<String, String> profile, String status) {
        EnrichedTx e = EnrichedTx.from(tx);
        e.lookupStatus = status;
        if (profile == null || profile.isEmpty()) {
            e.profileFound = false;
            
            return e;
        }
        e.profileFound           = true;
        e.kycLevel               = Integer.valueOf(profile.get("kyc_level"));
        e.age                    = Integer.valueOf(profile.get("age"));
        e.monthlyIncome          = Long.valueOf(profile.get("monthly_income"));
        e.cardLimit              = Long.valueOf(profile.get("card_limit"));
        e.avgAmount90d           = Long.valueOf(profile.get("avg_amount_90d"));
        e.avgAmount120d          = Long.valueOf(profile.get("avg_amount_120d"));
        e.avgAmount180d          = Long.valueOf(profile.get("avg_amount_180d"));
        e.riskScore              = Double.valueOf(profile.get("risk_score"));
        e.cicGroup               = Integer.valueOf(profile.get("cic_group"));
        e.historicalRiskScore    = Double.valueOf(profile.get("historical_risk_score"));
        e.hasTransferredBefore   = Boolean.valueOf(profile.get("has_transferred_before"));
        e.accountAgeDays         = Integer.valueOf(profile.get("account_age_days"));
        e.enrichedProfileVersion = Long.valueOf(profile.get("profile_version"));

        return e;
    }

    @Override
    public void close() {
        if (conn != null) {
            conn.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}
