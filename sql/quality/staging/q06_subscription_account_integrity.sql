SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_commercial_subscriptions s
LEFT JOIN auto_marketplace_staging.stg_commercial_accounts a
    ON s.dealer_id = a.dealer_id
WHERE
    s.dealer_id IS NOT NULL
    AND a.dealer_id IS NULL;
