#!/bin/bash
NUM_USERS=${NUM_USERS:-100000}

sqlplus -s bank/bank@//localhost:1521/XEPDB1 <<EOF
WHENEVER SQLERROR EXIT SQL.SQLCODE
CREATE TABLE users (
  user_id                VARCHAR2(32)  PRIMARY KEY,
  kyc_level              NUMBER(3)     NOT NULL,
  age                    NUMBER(3)     NOT NULL,
  monthly_income         NUMBER(19)    NOT NULL,
  card_limit             NUMBER(19)    NOT NULL,
  avg_amount_90d         NUMBER(19)    NOT NULL,
  avg_amount_120d        NUMBER(19)    NOT NULL,
  avg_amount_180d        NUMBER(19)    NOT NULL,
  risk_score             BINARY_DOUBLE NOT NULL,  -- do model offline chấm
  cic_group              NUMBER(1)     NOT NULL,  -- nhóm nợ CIC 1..5
  historical_risk_score  BINARY_DOUBLE NOT NULL,
  has_transferred_before NUMBER(1)     NOT NULL,  -- 0/1
  account_age_days       NUMBER(6)     NOT NULL,
  profile_version        NUMBER(19)    DEFAULT 1 NOT NULL,
  updated_at             TIMESTAMP(3)  DEFAULT SYSTIMESTAMP NOT NULL
);

ALTER TABLE users ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

EXEC DBMS_RANDOM.SEED(42);

INSERT /*+ APPEND */ INTO users
  (user_id, kyc_level, age, monthly_income, card_limit, avg_amount_90d, avg_amount_120d, avg_amount_180d,
   risk_score, cic_group, historical_risk_score, has_transferred_before, account_age_days)
SELECT 'usr_' || LEVEL,
       TRUNC(DBMS_RANDOM.VALUE(1, 4)),
       TRUNC(DBMS_RANDOM.VALUE(18, 76)),
       ROUND(DBMS_RANDOM.VALUE(5000000, 100000000), -5),
       ROUND(DBMS_RANDOM.VALUE(10000000, 500000000), -6),
       ROUND(DBMS_RANDOM.VALUE(100000, 20000000), -3),
       ROUND(DBMS_RANDOM.VALUE(100000, 20000000), -3),
       ROUND(DBMS_RANDOM.VALUE(100000, 20000000), -3),
       ROUND(DBMS_RANDOM.VALUE(0, 1), 4),
       TRUNC(DBMS_RANDOM.VALUE(1, 6)),
       ROUND(DBMS_RANDOM.VALUE(0, 1), 4),
       TRUNC(DBMS_RANDOM.VALUE(0, 2)),
       TRUNC(DBMS_RANDOM.VALUE(1, 3651))
FROM dual CONNECT BY LEVEL <= ${NUM_USERS};
COMMIT;

SELECT COUNT(*) AS seeded_users FROM users;
EXIT
EOF
