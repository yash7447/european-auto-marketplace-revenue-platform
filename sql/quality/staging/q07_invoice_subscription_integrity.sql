SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_commercial_invoices i
LEFT JOIN auto_marketplace_staging.stg_commercial_subscriptions s
    ON i.subscription_id = s.subscription_id
WHERE
    i.subscription_id IS NOT NULL
    AND s.subscription_id IS NULL;
