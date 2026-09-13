SELECT
    SUM(duplicate_rows) AS bad_rows
FROM
(
    SELECT COUNT(*) AS duplicate_rows
    FROM
    (
        SELECT vehicle_id
        FROM auto_marketplace_staging.stg_marketplace_vehicles
        GROUP BY vehicle_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT listing_id
        FROM auto_marketplace_staging.stg_marketplace_listings
        GROUP BY listing_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT dealer_id
        FROM auto_marketplace_staging.stg_commercial_accounts
        GROUP BY dealer_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT contract_id
        FROM auto_marketplace_staging.stg_commercial_contracts
        GROUP BY contract_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT subscription_id
        FROM auto_marketplace_staging.stg_commercial_subscriptions
        GROUP BY subscription_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT invoice_id
        FROM auto_marketplace_staging.stg_commercial_invoices
        GROUP BY invoice_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT product_id
        FROM auto_marketplace_staging.stg_pricing_products
        GROUP BY product_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT price_id
        FROM auto_marketplace_staging.stg_pricing_prices
        GROUP BY price_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT event_id
        FROM auto_marketplace_staging.stg_engagement_events
        GROUP BY event_id
        HAVING COUNT(*) > 1
    )

    UNION ALL

    SELECT COUNT(*)
    FROM
    (
        SELECT target_id
        FROM auto_marketplace_staging.stg_sales_targets
        GROUP BY target_id
        HAVING COUNT(*) > 1
    )
);
