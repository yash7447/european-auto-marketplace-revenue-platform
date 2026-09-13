SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_analytics.fact_subscription
WHERE dealer_id IS NOT NULL AND dealer_key IS NULL
   OR product_id IS NOT NULL AND product_key IS NULL
   OR subscription_start_date IS NOT NULL AND start_date_key IS NULL;
