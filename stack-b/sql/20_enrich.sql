-- Stack B: tx_events (Fluss) LEFT JOIN user_profile (Fluss, lookup theo PK) -> enriched_tx (Fluss)
-- Placeholder thay khi deploy: __LOOKUP_ASYNC__ (true/false), __FLUSS_BATCH_TIMEOUT__ (vd 5ms),
-- __FLUSS_FETCH_WAIT__ (thời gian chờ tối đa mỗi lần fetch log, mặc định Fluss 500ms)
INSERT INTO fluss_catalog.fraud.enriched_tx
  /*+ OPTIONS('client.writer.batch-timeout' = '__FLUSS_BATCH_TIMEOUT__') */
SELECT
  tx.transaction_id, tx.user_id, tx.amount, tx.currency, tx.merchant_id,
  tx.device_id, tx.`timestamp`,
  u.kyc_level, u.age, u.monthly_income, u.card_limit,
  u.avg_amount_90d, u.avg_amount_120d, u.avg_amount_180d,
  u.risk_score, u.cic_group, u.historical_risk_score,
  u.has_transferred_before, u.account_age_days,
  u.profile_version AS enriched_profile_version,
  u.user_id IS NOT NULL AS profile_found,
  tx.send_ts_ms, tx.expected_profile_version
FROM (
  SELECT *, PROCTIME() AS proc_time
  FROM fluss_catalog.fraud.tx_events
    /*+ OPTIONS('scan.startup.mode' = 'latest',
                'client.scanner.log.fetch.wait-max-time' = '__FLUSS_FETCH_WAIT__') */
) AS tx
LEFT JOIN fluss_catalog.fraud.user_profile
  /*+ OPTIONS('lookup.async' = '__LOOKUP_ASYNC__',
              'client.lookup.batch-timeout' = '__FLUSS_BATCH_TIMEOUT__') */
  FOR SYSTEM_TIME AS OF tx.proc_time AS u
  ON tx.user_id = u.user_id;
