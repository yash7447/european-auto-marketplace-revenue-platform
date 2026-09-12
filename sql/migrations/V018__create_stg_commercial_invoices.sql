CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_invoices
(
    invoice_id string,
    subscription_id string,
    dealer_id string,

    invoice_date date,

    invoice_amount decimal(14,2),

    currency string,
    payment_status string,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    month(invoice_date)
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/invoices/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
