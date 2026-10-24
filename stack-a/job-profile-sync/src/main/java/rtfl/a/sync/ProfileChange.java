package rtfl.a.sync;

import com.fasterxml.jackson.databind.JsonNode;
import rtfl.common.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public class ProfileChange {
    public String userId;
    public boolean delete;
    public long version;
    public Map<String, String> fields;

    public ProfileChange() {}

    public static ProfileChange parse(String message) {
        JsonNode root   = Json.tree(message);
        String op       = root.path("op").asText("");
        ProfileChange c = new ProfileChange();
        JsonNode row;

        switch (op) {
            case "c": case "u": case "r":
                row = root.path("after");
                break;
            case "d":
                row = root.path("before");
                c.delete = true;
                break;
            default:
                return null;
        }
        if (row.isMissingNode() || row.isNull()) {
            return null;
        }
        c.userId  = row.path("USER_ID").asText();
        c.version = Long.parseLong(row.path("PROFILE_VERSION").asText());
        if (!c.delete) {
            c.fields = new LinkedHashMap<>();
            c.fields.put("kyc_level",              row.path("KYC_LEVEL").asText());
            c.fields.put("age",                    row.path("AGE").asText());
            c.fields.put("monthly_income",         row.path("MONTHLY_INCOME").asText());
            c.fields.put("card_limit",             row.path("CARD_LIMIT").asText());
            c.fields.put("avg_amount_90d",         row.path("AVG_AMOUNT_90D").asText());
            c.fields.put("avg_amount_120d",        row.path("AVG_AMOUNT_120D").asText());
            c.fields.put("avg_amount_180d",        row.path("AVG_AMOUNT_180D").asText());
            c.fields.put("risk_score",             row.path("RISK_SCORE").asText());
            c.fields.put("cic_group",              row.path("CIC_GROUP").asText());
            c.fields.put("historical_risk_score",  row.path("HISTORICAL_RISK_SCORE").asText());
            // NUMBER(1) 0/1 -> "true"/"false"
            c.fields.put("has_transferred_before", Boolean.toString(row.path("HAS_TRANSFERRED_BEFORE").asInt() == 1));
            c.fields.put("account_age_days",       row.path("ACCOUNT_AGE_DAYS").asText());
            c.fields.put("profile_version",        Long.toString(c.version));
            c.fields.put("updated_at",             row.path("UPDATED_AT").asText());
        }
        return c;
    }
}
