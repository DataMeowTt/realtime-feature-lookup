CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);

CREATE DATABASE IF NOT EXISTS fluss_catalog.fraud;
