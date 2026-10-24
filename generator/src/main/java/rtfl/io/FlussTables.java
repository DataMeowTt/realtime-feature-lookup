package rtfl.io;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import rtfl.common.EnrichedTx;
import rtfl.common.TxEvent;

public final class FlussTables {
    public static final TablePath TX_EVENTS = TablePath.of("fraud", "tx_events");
    public static final TablePath ENRICHED_TX = TablePath.of("fraud", "enriched_tx");
    public static final TablePath USER_PROFILE = TablePath.of("fraud", "user_profile");
    /** Vị trí cột profile_version trong user_profile. */
    public static final int PROFILE_VERSION_POS = 13;

    private FlussTables() {}

    public static Connection connect(Args.Endpoints ep) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", ep.fluss());
        // Mặc định client gom batch 100ms cho ghi và lookup -> đặt bằng giá trị dùng ở phía Flink
        conf.setString("client.writer.batch-timeout", ep.flussBatchTimeout());
        conf.setString("client.lookup.batch-timeout", ep.flussBatchTimeout());
        // Mặc định scanner chờ tối đa 500ms mỗi lần fetch
        conf.setString("client.scanner.log.fetch.wait-max-time", ep.flussFetchWait());
        return ConnectionFactory.createConnection(conf);
    }

    public static InternalRow txRow(TxEvent tx) {
        return GenericRow.of(
                s(tx.transactionId), s(tx.userId), tx.amount, s(tx.currency), s(tx.merchantId),
                s(tx.deviceId), s(tx.timestamp), tx.sendTsMs, tx.expectedProfileVersion);
    }

    public static EnrichedTx enriched(InternalRow r) {
        EnrichedTx e = new EnrichedTx();
        e.transactionId = str(r, 0);
        e.userId = str(r, 1);
        e.amount = r.getLong(2);
        e.currency = str(r, 3);
        e.merchantId = str(r, 4);
        e.deviceId = str(r, 5);
        e.timestamp = str(r, 6);
        e.kycLevel = r.isNullAt(7) ? null : r.getInt(7);
        e.age = r.isNullAt(8) ? null : r.getInt(8);
        e.monthlyIncome = r.isNullAt(9) ? null : r.getLong(9);
        e.cardLimit = r.isNullAt(10) ? null : r.getLong(10);
        e.avgAmount90d = r.isNullAt(11) ? null : r.getLong(11);
        e.avgAmount120d = r.isNullAt(12) ? null : r.getLong(12);
        e.avgAmount180d = r.isNullAt(13) ? null : r.getLong(13);
        e.riskScore = r.isNullAt(14) ? null : r.getDouble(14);
        e.cicGroup = r.isNullAt(15) ? null : r.getInt(15);
        e.historicalRiskScore = r.isNullAt(16) ? null : r.getDouble(16);
        e.hasTransferredBefore = r.isNullAt(17) ? null : r.getBoolean(17);
        e.accountAgeDays = r.isNullAt(18) ? null : r.getInt(18);
        e.enrichedProfileVersion = r.isNullAt(19) ? null : r.getLong(19);
        e.profileFound = !r.isNullAt(20) && r.getBoolean(20);
        e.sendTsMs = r.getLong(21);
        e.expectedProfileVersion = r.getLong(22);
        return e;
    }

    public static BinaryString s(String v) {
        return v == null ? null : BinaryString.fromString(v);
    }

    public static String str(InternalRow r, int pos) {
        return r.isNullAt(pos) ? null : r.getString(pos).toString();
    }
}
