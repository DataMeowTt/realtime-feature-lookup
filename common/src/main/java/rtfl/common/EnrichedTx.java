package rtfl.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Output enrichment (enriched_tx). Cột profile là null khi không tìm thấy user. */
public class EnrichedTx {
    public String transactionId;
    public String userId;
    public long amount;
    public String currency;
    public String merchantId;
    public String deviceId;
    public String timestamp;

    public Integer kycLevel;
    public Integer age;
    public Long monthlyIncome;
    public Long cardLimit;
    // SNAKE_CASE không tách chữ số ("avg_amount90d") -> đặt tên tường minh
    @JsonProperty("avg_amount_90d")
    public Long avgAmount90d;
    @JsonProperty("avg_amount_120d")
    public Long avgAmount120d;
    @JsonProperty("avg_amount_180d")
    public Long avgAmount180d;
    public Double riskScore;
    public Integer cicGroup;
    public Double historicalRiskScore;
    public Boolean hasTransferredBefore;
    public Integer accountAgeDays;
    public Long enrichedProfileVersion;

    public boolean profileFound;
    /** Chỉ Stack A: OK / TIMEOUT / ERROR. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String lookupStatus;

    public long sendTsMs;
    public long expectedProfileVersion;

    public EnrichedTx() {}

    public static EnrichedTx from(TxEvent tx) {
        EnrichedTx e = new EnrichedTx();
        e.transactionId = tx.transactionId;
        e.userId = tx.userId;
        e.amount = tx.amount;
        e.currency = tx.currency;
        e.merchantId = tx.merchantId;
        e.deviceId = tx.deviceId;
        e.timestamp = tx.timestamp;
        e.sendTsMs = tx.sendTsMs;
        e.expectedProfileVersion = tx.expectedProfileVersion;
        return e;
    }

    @JsonIgnore
    public boolean isStale() {
        if (expectedProfileVersion < 0) {
            return profileFound;
        }
        return !profileFound || enrichedProfileVersion == null || enrichedProfileVersion < expectedProfileVersion;
    }
}
