SELECT COUNT(*) AS bad_rows
FROM auto_marketplace_staging.stg_commercial_invoices i
LEFT JOIN auto_marketplace_staging.stg_commercial_accounts a
    ON i.dealer_id = a.dealer_id
WHERE
    i.dealer_id IS NOT NULL
    AND a.dealer_id IS NULL;
