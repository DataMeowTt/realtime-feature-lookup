package rtfl.common;

public class TxEvent {
    public String transactionId;
    public String userId;
    public long amount;
    public String currency;
    public String merchantId;
    public String deviceId;
    public String timestamp;
    
    // Trường phục vụ đo đạc, do generator gắn vào
    public long sendTsMs;

    /** 
     * Version mới nhất generator biết đã commit; 
     * -1 = user không tồn tại / đã bị xoá. 
     **/
    public long expectedProfileVersion;

    public TxEvent() {}
}
