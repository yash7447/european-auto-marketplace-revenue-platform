SELECT
    SUM(bad_rows) AS bad_rows
FROM
(
    SELECT COUNT(*) AS bad_rows
    FROM auto_marketplace_staging.stg_marketplace_vehicles
    WHERE vehicle_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_marketplace_listings
    WHERE listing_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_commercial_accounts
    WHERE dealer_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_commercial_contracts
    WHERE contract_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_commercial_subscriptions
    WHERE subscription_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_commercial_invoices
    WHERE invoice_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_pricing_products
    WHERE product_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_pricing_prices
    WHERE price_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_engagement_events
    WHERE event_id IS NULL

    UNION ALL

    SELECT COUNT(*)
    FROM auto_marketplace_staging.stg_sales_targets
    WHERE target_id IS NULL
);
