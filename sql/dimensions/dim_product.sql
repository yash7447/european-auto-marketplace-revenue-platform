MERGE INTO auto_marketplace_analytics.dim_product t
USING (
    SELECT
        to_hex(md5(to_utf8(product_id))) AS product_key,
        product_id,
        product_name,
        product_family,
        billing_frequency,
        active,
        updated_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_staging.stg_pricing_products
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
    product_key=s.product_key,
    product_name=s.product_name,
    product_family=s.product_family,
    billing_frequency=s.billing_frequency,
    active=s.active,
    updated_at=s.updated_at,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    product_key, product_id, product_name, product_family,
    billing_frequency, active, updated_at, loaded_at
) VALUES (
    s.product_key, s.product_id, s.product_name, s.product_family,
    s.billing_frequency, s.active, s.updated_at, s.loaded_at
);
