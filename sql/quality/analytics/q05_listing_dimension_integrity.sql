SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_analytics.fact_listing_performance
WHERE dealer_id IS NOT NULL AND dealer_key IS NULL
   OR vehicle_id IS NOT NULL AND vehicle_key IS NULL
   OR listing_start_date IS NOT NULL AND start_date_key IS NULL;
