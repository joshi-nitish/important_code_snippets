CREATE OR REPLACE EXTERNAL TABLE dh-darkstores-stg.dev_dmart.rooster_zone_warehouse_mapping
  OPTIONS (
  format = 'CSV',
  uris = ['gs://qc-sos-wfm-dev/qc_sos_dmart_configs_prod/*']);
