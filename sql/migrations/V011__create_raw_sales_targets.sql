CREATE EXTERNAL TABLE IF NOT EXISTS
auto_marketplace_raw.raw_sales_targets
(
    target_id STRING,
    target_month STRING,
    market STRING,
    sales_rep STRING,
    product_group STRING,
    revenue_target_eur STRING,
    upsell_target_eur STRING,
    new_dealer_target STRING,
    retention_target_pct STRING,
    planning_version STRING,
    created_at STRING
)
PARTITIONED BY
(
    sha256 STRING
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar'=',',
    'quoteChar'='"'
)
STORED AS TEXTFILE
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/raw/sales_targets/files/'
TBLPROPERTIES
(
    'skip.header.line.count'='1'
)
