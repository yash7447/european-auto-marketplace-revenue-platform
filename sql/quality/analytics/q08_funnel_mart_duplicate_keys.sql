SELECT COUNT(*) AS bad_rows
FROM (
    SELECT funnel_key
    FROM auto_marketplace_analytics.mart_marketplace_funnel_monthly
    GROUP BY funnel_key
    HAVING COUNT(*) > 1
);
