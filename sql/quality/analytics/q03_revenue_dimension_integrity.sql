SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_analytics.fact_revenue
WHERE dealer_id IS NOT NULL AND dealer_key IS NULL
   OR product_id IS NOT NULL AND product_key IS NULL
   OR invoice_date IS NOT NULL AND date_key IS NULL;
