SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_pricing_prices p
LEFT JOIN auto_marketplace_staging.stg_pricing_products product
    ON p.product_id = product.product_id
WHERE
    p.product_id IS NOT NULL
    AND product.product_id IS NULL;
