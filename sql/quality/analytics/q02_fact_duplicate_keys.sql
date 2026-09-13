SELECT
    (
        SELECT COUNT(*) FROM (
            SELECT invoice_id FROM auto_marketplace_analytics.fact_revenue
            GROUP BY invoice_id HAVING COUNT(*) > 1
        )
    )
    +
    (
        SELECT COUNT(*) FROM (
            SELECT subscription_id FROM auto_marketplace_analytics.fact_subscription
            GROUP BY subscription_id HAVING COUNT(*) > 1
        )
    )
    +
    (
        SELECT COUNT(*) FROM (
            SELECT listing_id FROM auto_marketplace_analytics.fact_listing_performance
            GROUP BY listing_id HAVING COUNT(*) > 1
        )
    )
    +
    (
        SELECT COUNT(*) FROM (
            SELECT target_id FROM auto_marketplace_analytics.fact_sales_target
            GROUP BY target_id HAVING COUNT(*) > 1
        )
    ) AS bad_rows;
