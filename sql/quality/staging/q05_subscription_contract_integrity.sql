SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_commercial_subscriptions s
LEFT JOIN auto_marketplace_staging.stg_commercial_contracts c
    ON s.contract_id = c.contract_id
WHERE
    s.contract_id IS NOT NULL
    AND c.contract_id IS NULL;
