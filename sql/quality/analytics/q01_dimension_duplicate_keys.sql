SELECT
    (
        SELECT COUNT(*) FROM (
            SELECT dealer_id FROM auto_marketplace_analytics.dim_dealer
            GROUP BY dealer_id HAVING COUNT(*) > 1
        )
    )
    +
    (
        SELECT COUNT(*) FROM (
            SELECT product_id FROM auto_marketplace_analytics.dim_product
            GROUP BY product_id HAVING COUNT(*) > 1
        )
    )
    +
    (
        SELECT COUNT(*) FROM (
            SELECT vehicle_id FROM auto_marketplace_analytics.dim_vehicle
            GROUP BY vehicle_id HAVING COUNT(*) > 1
        )
    )
    +
    (
        SELECT COUNT(*) FROM (
            SELECT date_key FROM auto_marketplace_analytics.dim_date
            GROUP BY date_key HAVING COUNT(*) > 1
        )
    ) AS bad_rows;
