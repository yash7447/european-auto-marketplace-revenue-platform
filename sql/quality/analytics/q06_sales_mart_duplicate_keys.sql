SELECT COUNT(*) AS bad_rows
FROM (
    SELECT performance_key
    FROM auto_marketplace_analytics.mart_sales_performance_monthly
    GROUP BY performance_key
    HAVING COUNT(*) > 1
);
