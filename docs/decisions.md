# Nhật ký quyết định thiết kế

## Phiên bản (K1)

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Flink | 1.20.5 (image `apache/flink:1.20.5-java17`) | dùng chung cho cả hai stack |
| Fluss | 1.0.0 (`apache/fluss:1.0.0`, `fluss-flink-1.20:1.0.0`, `fluss-client:1.0.0`) | |
| Flink CDC | `flink-sql-connector-oracle-cdc:3.6.0-1.20` | nhúng Debezium 1.9 |
| Debezium (Stack A) | `quay.io/debezium/connect:3.7.0.Final` | đã có sẵn ojdbc11 |
| Kafka | `apache/kafka:4.1.0` (KRaft, 1 broker) | connector Flink `3.4.0-1.20` |
| Redis | 7.4, client Lettuce 6.4.2 | |
| Oracle | XE 21c (`gvenzl/oracle-xe:21-slim`, chạy amd64 giả lập trên Apple Silicon) | |

## Oracle XE 21c thay vì 23ai/26ai
Debezium 1.9 nhúng trong Flink CDC 3.6 không parse được banner phiên bản nhiều dòng của Oracle 23
(`Failed to resolve Oracle database version`). XE 21c chạy được với cả Debezium 3.7 (A) và Flink CDC (B).

## Driver JDBC cho Flink: ojdbc8 19.3
ojdbc 23.x gây `ORA-18716 ... not in any time zone` khi Flink CDC đọc SCN timestamp.

## Flink CDC: tắt incremental snapshot
`scan.incremental.snapshot.enabled = false`. Incremental snapshot lỗi
"db history topic ... missing" với Oracle PDB. Hệ quả: source CDC của Stack B chạy parallelism 1.
Ở mức ~10 update/s không ảnh hưởng, nhưng snapshot ban đầu chậm hơn.

## Debezium: decimal.handling.mode = string, tombstones.on.delete = false
Mặc định `NUMBER(19)` được mã hoá base64 (kiểu Decimal). Chuyển sang chuỗi để Job A1 parse trực tiếp.
Tắt tombstone vì Job A1 đã xử lý `op=d`.

## Delivery guarantee: at-least-once cho cả hai stack
Checkpoint 10s, mode AT_LEAST_ONCE. Kafka sink `AT_LEAST_ONCE`, Fluss sink mặc định.
latency-consumer loại bản trùng theo `transaction_id` và báo tỷ lệ trùng.

## Fluss client batch-timeout = 5ms (mặc định 100ms)
Fluss client mặc định gom batch 100ms cho cả ghi (`client.writer.batch-timeout`) và lookup
(`client.lookup.batch-timeout`). Với giá trị mặc định, mỗi lookup/ghi có thể chờ tới 100ms.
Dự án đặt 5ms (biến `FLUSS_BATCH_TIMEOUT`) qua SQL hint ở CDC sink, lookup join, sink enriched_tx,
và ở generator/consumer/probe. Nên chạy thêm một lần với `FLUSS_BATCH_TIMEOUT=100ms` để báo cáo
ảnh hưởng của cấu hình mặc định.

## Fluss scanner fetch.wait-max-time = 10ms (mặc định 500ms)
`client.scanner.log.fetch.wait-max-time` mặc định 500ms. Đo thử B ở 1000 tx/s:
mặc định cho p50 ≈ 460ms (hai chặng đọc log: Flink đọc `tx_events`, consumer đọc `enriched_tx`);
đặt 10ms cho p50 ≈ 23ms. Dự án đặt 10ms (biến `FLUSS_FETCH_WAIT`) ở source `tx_events` của B
và ở consumer/tests. Kafka phía A dùng mặc định (`fetch.min.bytes=1` trả về ngay khi có dữ liệu),
consumer Kafka đặt `fetch.max.wait.ms=5`. Đây là phát hiện nên đưa vào báo cáo: cấu hình mặc định
của Fluss thiên về throughput, phải tinh chỉnh khi cần độ trễ thấp.

## Partition nóng thay cho Zipf (X3)
Zipf s=1.2 dồn ~20% tx vào một user (vài nghìn tx/s cho một người) — không thực tế.
Thay bằng "partition nóng": dồn `hot-share` (mặc định 80%) tx vào các user thuộc một partition,
chọn đều trong nhóm (~25k user với 4 partition), nên mỗi user vẫn giao dịch bình thường.
Partition tính bằng đúng hàm băm của hệ thống: murmur2 của Kafka (A: topic tx_events)
và KeyEncoder + BucketingFunction của Fluss (B: bucket user_profile; đã đối chiếu 8000/8000
bản ghi khớp bucket thật). Chế độ đổi khi đang chạy qua `bench/control/generator.conf`
(`make hot` / `make normal`), generator đọc lại mỗi giây.

## Kafka producer linger
Generator `linger.ms=1`; sink của Job A2 `linger.ms=0`.

## Cột `timestamp` là STRING
Enrichment không dùng thời gian sự kiện, chỉ chuyển tiếp. Để STRING giúp JSON (Kafka) và Fluss giống nhau.

## Chỉ xử lý tx mới
Job A2 (`latest()`), B (`scan.startup.mode=latest`). Consumer cũng bắt đầu từ cuối và lọc theo `send_ts_ms`.

## expected_profile_version
Generator nạp version hiện tại của mọi user từ Oracle khi khởi động, sau đó chỉ cập nhật
map sau khi UPDATE đã commit. `-1` = user không tồn tại hoặc đã xoá.
Stale = (expected = -1 và profile_found) hoặc (expected ≥ 0 và (không tìm thấy hoặc version < expected)).

## Deploy luôn từ trạng thái sạch
`infra/scripts/deploy.sh` huỷ job cũ, xoá/tạo lại bảng Fluss (Stack B) hoặc FLUSHALL Redis
+ xoá consumer group `profile-sync` (Stack A), rồi chờ feature store có đủ `NUM_USERS` dòng (K6).

## Công cụ chạy trong container `tools`
Generator, consumer, probe, tests chạy bằng `docker compose run tools` trong network `rtfl_default`
nên dùng được tên service (`kafka:9092`, `coordinator-server:9123`, ...) và chung đồng hồ (K2).

## Khác biệt còn lại giữa hai stack (ghi trong báo cáo)
- CDC: Debezium 3.7 (A) vs Debezium 1.9 nhúng trong Flink CDC (B); cả hai dùng LogMiner `online_catalog`.
- Async I/O của A dùng `unorderedWait` mặc định (K4); lookup join của Fluss mặc định async.
- Dữ liệu Fluss không có volume: mất khi `make down`, deploy lại sẽ nạp lại từ Oracle.
