#!/bin/bash
# Bảng giao dịch đầu vào (TxEvent) và giao dịch đã enrich (EnrichedTx), khởi tạo rỗng.

sqlplus -s bank/bank@//localhost:1521/XEPDB1 <<EOF
WHENEVER SQLERROR EXIT SQL.SQLCODE
CREATE TABLE tx_events (
  transaction_id           VARCHAR2(64)  PRIMARY KEY,
  user_id                  VARCHAR2(32)  NOT NULL,
  amount                   NUMBER(19)    NOT NULL,
  currency                 VARCHAR2(3)   NOT NULL,
  merchant_id              VARCHAR2(32),
  device_id                VARCHAR2(64),
  timestamp                VARCHAR2(40),
  send_ts_ms               NUMBER(19),
  expected_profile_version NUMBER(19)
);

CREATE TABLE enriched_tx (
  transaction_id           VARCHAR2(64)  PRIMARY KEY,
  user_id                  VARCHAR2(32)  NOT NULL,
  amount                   NUMBER(19)    NOT NULL,
  currency                 VARCHAR2(3)   NOT NULL,
  merchant_id              VARCHAR2(32),
  device_id                VARCHAR2(64),
  timestamp                VARCHAR2(40),
  -- Cột profile: NULL khi không tìm thấy user
  kyc_level                NUMBER(3),
  age                      NUMBER(3),
  monthly_income           NUMBER(19),
  card_limit               NUMBER(19),
  avg_amount_90d           NUMBER(19),
  avg_amount_120d          NUMBER(19),
  avg_amount_180d          NUMBER(19),
  risk_score               BINARY_DOUBLE,
  cic_group                NUMBER(1),
  historical_risk_score    BINARY_DOUBLE,
  has_transferred_before   NUMBER(1),    -- 0/1
  account_age_days         NUMBER(6),
  enriched_profile_version NUMBER(19),
  profile_found            NUMBER(1)     NOT NULL,  -- 0/1
  lookup_status            VARCHAR2(16),            -- OK / TIMEOUT / ERROR
  send_ts_ms               NUMBER(19),
  expected_profile_version NUMBER(19)
);
EXIT
EOF
