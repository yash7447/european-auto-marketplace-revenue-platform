MERGE INTO auto_marketplace_intermediate.int_price_book t
USING (
    SELECT
        pr.price_id,
        pr.product_id,
        p.product_name,
        p.product_family,
        pr.market,
        pr.dealer_segment,
        pr.currency,
        pr.list_price,
        pr.effective_from,
        pr.effective_to,
        pr.pricing_version,
        p.active AS product_active,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at
    FROM auto_marketplace_staging.stg_pricing_prices pr
    LEFT JOIN auto_marketplace_staging.stg_pricing_products p
        ON pr.product_id = p.product_id
) s
ON t.price_id = s.price_id
WHEN MATCHED THEN UPDATE SET
    product_id=s.product_id,
    product_name=s.product_name,
    product_family=s.product_family,
    market=s.market,
    dealer_segment=s.dealer_segment,
    currency=s.currency,
    list_price=s.list_price,
    effective_from=s.effective_from,
    effective_to=s.effective_to,
    pricing_version=s.pricing_version,
    product_active=s.product_active,
    transformed_at=s.transformed_at
WHEN NOT MATCHED THEN INSERT (
    price_id, product_id, product_name, product_family, market, dealer_segment,
    currency, list_price, effective_from, effective_to, pricing_version,
    product_active, transformed_at
) VALUES (
    s.price_id, s.product_id, s.product_name, s.product_family, s.market,
    s.dealer_segment, s.currency, s.list_price, s.effective_from,
    s.effective_to, s.pricing_version, s.product_active, s.transformed_at
);
