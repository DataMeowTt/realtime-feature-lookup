-- Oracle (LogMiner) -> Fluss user_profile.
CREATE TEMPORARY TABLE oracle_users (
  USER_ID             STRING,
  KYC_LEVEL              DECIMAL(3, 0),
  AGE                    DECIMAL(3, 0),
  MONTHLY_INCOME         DECIMAL(19, 0),
  CARD_LIMIT             DECIMAL(19, 0),
  AVG_AMOUNT_90D         DECIMAL(19, 0),
  AVG_AMOUNT_120D        DECIMAL(19, 0),
  AVG_AMOUNT_180D        DECIMAL(19, 0),
  RISK_SCORE             DOUBLE,
  CIC_GROUP              DECIMAL(1, 0),
  HISTORICAL_RISK_SCORE  DOUBLE,
  HAS_TRANSFERRED_BEFORE DECIMAL(1, 0),
  ACCOUNT_AGE_DAYS       DECIMAL(6, 0),
  PROFILE_VERSION        DECIMAL(19, 0),
  UPDATED_AT          TIMESTAMP(3),
  PRIMARY KEY (USER_ID) NOT ENFORCED
) WITH (
  'connector' = 'oracle-cdc',
  'hostname' = 'oracle',
  'port' = '1521',
  'username' = 'c##dbzuser',
  'password' = 'dbz',
  'database-name' = 'XE',
  'schema-name' = 'BANK',
  'table-name' = 'USERS',
  'debezium.database.pdb.name' = 'XEPDB1',
  'debezium.log.mining.strategy' = 'online_catalog',
  'scan.incremental.snapshot.enabled' = 'false'
);

INSERT INTO fluss_catalog.fraud.user_profile
SELECT USER_ID,
       CAST(KYC_LEVEL AS INT),
       CAST(AGE AS INT),
       CAST(MONTHLY_INCOME AS BIGINT),
       CAST(CARD_LIMIT AS BIGINT),
       CAST(AVG_AMOUNT_90D AS BIGINT),
       CAST(AVG_AMOUNT_120D AS BIGINT),
       CAST(AVG_AMOUNT_180D AS BIGINT),
       RISK_SCORE,
       CAST(CIC_GROUP AS INT),
       HISTORICAL_RISK_SCORE,
       HAS_TRANSFERRED_BEFORE = 1,
       CAST(ACCOUNT_AGE_DAYS AS INT),
       CAST(PROFILE_VERSION AS BIGINT),
       UPDATED_AT
FROM oracle_users;
