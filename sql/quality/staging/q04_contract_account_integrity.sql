SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_commercial_contracts c
LEFT JOIN auto_marketplace_staging.stg_commercial_accounts a
    ON c.dealer_id = a.dealer_id
WHERE
    c.dealer_id IS NOT NULL
    AND a.dealer_id IS NULL;
