SELECT COUNT(*) AS bad_rows
FROM (
    SELECT product_month_key
    FROM auto_marketplace_analytics.mart_product_performance_monthly
    GROUP BY product_month_key
    HAVING COUNT(*) > 1
);
