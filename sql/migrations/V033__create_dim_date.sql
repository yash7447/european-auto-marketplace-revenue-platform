CREATE TABLE IF NOT EXISTS auto_marketplace_analytics.dim_date (
    date_key int,
    calendar_date date,
    year_number int,
    quarter_number int,
    month_number int,
    month_name string,
    year_month string,
    week_of_year int,
    day_of_month int,
    day_of_week int,
    day_name string,
    is_weekend boolean,
    loaded_at timestamp
)
LOCATION 's3://european-auto-marketplace-data-yash-2026-01/analytics/dimensions/dim_date/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
