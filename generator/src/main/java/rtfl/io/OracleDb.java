package rtfl.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class OracleDb implements AutoCloseable {
    private final Connection conn;

    public OracleDb(Args.Endpoints ep) throws SQLException {
        conn = DriverManager.getConnection(ep.oracleUrl(), ep.oracleUser(), ep.oraclePassword());
        conn.setAutoCommit(false);
    }

    public Map<String, Long> loadVersions() throws SQLException {
        Map<String, Long> versions = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT user_id, profile_version FROM users")) {
            ps.setFetchSize(10_000);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    versions.put(rs.getString(1), rs.getLong(2));
                }
            }
        }
        conn.commit();
        return versions;
    }

    /** Giả lập model offline chấm lại risk_score. */
    public synchronized long update(String userId, double riskScore) throws SQLException {
        String sql = "UPDATE users SET risk_score = ?,"
                + " profile_version = profile_version + 1, updated_at = SYSTIMESTAMP WHERE user_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setDouble(1, riskScore);
            ps.setString(2, userId);
            if (ps.executeUpdate() == 0) {
                conn.commit();
                return -1;
            }
        }
        long v = version(userId);
        conn.commit();
        return v;
    }

    public synchronized void insert(String userId) throws SQLException {
        String sql = "INSERT INTO users (user_id, kyc_level, age, monthly_income, card_limit, avg_amount_90d,"
                + " avg_amount_120d, avg_amount_180d, risk_score, cic_group, historical_risk_score,"
                + " has_transferred_before, account_age_days)"
                + " VALUES (?, 1, 30, 20000000, 50000000, 2000000, 2000000, 2000000, 0.5, 1, 0.5, 0, 1)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            ps.executeUpdate();
        }
        conn.commit();
    }

    public synchronized long ensureUser(String userId) throws SQLException {
        long v = version(userId);
        if (v < 0) {
            insert(userId);
            v = version(userId);
        }
        conn.commit();
        return v;
    }

    public synchronized void delete(String userId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM users WHERE user_id = ?")) {
            ps.setString(1, userId);
            ps.executeUpdate();
        }
        conn.commit();
    }

    public synchronized long version(String userId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT profile_version FROM users WHERE user_id = ?")) {
            ps.setString(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }

    public synchronized Map<String, Object> profile(String userId) throws SQLException {
        String sql = "SELECT kyc_level, age, monthly_income, card_limit, avg_amount_90d, avg_amount_120d, avg_amount_180d,"
                + " risk_score, cic_group, historical_risk_score, has_transferred_before, account_age_days, profile_version"
                + " FROM users WHERE user_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    conn.commit();
                    return null;
                }
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("kyc_level", rs.getInt(1));
                m.put("age", rs.getInt(2));
                m.put("monthly_income", rs.getLong(3));
                m.put("card_limit", rs.getLong(4));
                m.put("avg_amount_90d", rs.getLong(5));
                m.put("avg_amount_120d", rs.getLong(6));
                m.put("avg_amount_180d", rs.getLong(7));
                m.put("risk_score", rs.getDouble(8));
                m.put("cic_group", rs.getInt(9));
                m.put("historical_risk_score", rs.getDouble(10));
                m.put("has_transferred_before", rs.getInt(11) == 1);
                m.put("account_age_days", rs.getInt(12));
                m.put("profile_version", rs.getLong(13));
                conn.commit();
                return m;
            }
        }
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}
