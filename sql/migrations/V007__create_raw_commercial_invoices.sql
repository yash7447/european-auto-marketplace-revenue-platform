CREATE EXTERNAL TABLE IF NOT EXISTS
auto_marketplace_raw.raw_commercial_invoices
(
    raw_json STRING
)
PARTITIONED BY
(
    extract_date STRING,
    run_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/raw/commercial/invoices/'
