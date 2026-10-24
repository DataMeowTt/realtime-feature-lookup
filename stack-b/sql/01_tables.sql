-- __BUCKETS__ được thay bằng biến BUCKETS khi deploy (infra/scripts/deploy.sh)
-- Khi có PRIMARY KEY, Fluss tự động kích hoạt cả Log Store (CDC) lẫn KV Store (RocksDB Lookup)
CREATE TABLE IF NOT EXISTS fluss_catalog.fraud.user_profile (
  user_id             STRING,
  kyc_level              INT,
  age                    INT,
  monthly_income         BIGINT,
  card_limit             BIGINT,
  avg_amount_90d         BIGINT,
  avg_amount_120d        BIGINT,
  avg_amount_180d        BIGINT,
  risk_score             DOUBLE,
  cic_group              INT,
  historical_risk_score  DOUBLE,
  has_transferred_before BOOLEAN,
  account_age_days       INT,
  profile_version        BIGINT,
  updated_at          TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket.num' = '__BUCKETS__',
  'table.log.ttl' = '7d'              -- Log store giữ changelog 7 ngày để replay
  -- Block cache RocksDB cho lookup là cấu hình tablet-server (kv.rocksdb.block.cache-size),
  -- không phải table property: xem infra/compose.stack-b.yml
  );

-- Log table: luồng giao dịch đầu vào
CREATE TABLE IF NOT EXISTS fluss_catalog.fraud.tx_events (
  transaction_id           STRING,
  user_id                  STRING,
  amount                   BIGINT,
  currency                 STRING,
  merchant_id              STRING,
  device_id                STRING,
  `timestamp`              STRING,
  send_ts_ms               BIGINT,
  expected_profile_version BIGINT
) WITH ('bucket.num' = '__BUCKETS__');

-- Log table: output enrichment
CREATE TABLE IF NOT EXISTS fluss_catalog.fraud.enriched_tx (
  transaction_id           STRING,
  user_id                  STRING,
  amount                   BIGINT,
  currency                 STRING,
  merchant_id              STRING,
  device_id                STRING,
  `timestamp`              STRING,
  kyc_level                INT,
  age                      INT,
  monthly_income           BIGINT,
  card_limit               BIGINT,
  avg_amount_90d           BIGINT,
  avg_amount_120d          BIGINT,
  avg_amount_180d          BIGINT,
  risk_score               DOUBLE,
  cic_group                INT,
  historical_risk_score    DOUBLE,
  has_transferred_before   BOOLEAN,
  account_age_days         INT,
  enriched_profile_version BIGINT,
  profile_found            BOOLEAN,
  send_ts_ms               BIGINT,
  expected_profile_version BIGINT
) WITH ('bucket.num' = '__BUCKETS__');
