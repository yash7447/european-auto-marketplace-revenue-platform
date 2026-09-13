MERGE INTO auto_marketplace_analytics.dim_dealer t
USING (
    SELECT
        to_hex(md5(to_utf8(dealer_id))) AS dealer_key,
        dealer_id,
        dealer_name,
        market,
        segment,
        sales_rep,
        account_status,
        created_date,
        updated_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
    FROM auto_marketplace_staging.stg_commercial_accounts
) s
ON t.dealer_id = s.dealer_id
WHEN MATCHED THEN UPDATE SET
    dealer_key=s.dealer_key,
    dealer_name=s.dealer_name,
    market=s.market,
    segment=s.segment,
    sales_rep=s.sales_rep,
    account_status=s.account_status,
    created_date=s.created_date,
    updated_at=s.updated_at,
    loaded_at=s.loaded_at
WHEN NOT MATCHED THEN INSERT (
    dealer_key, dealer_id, dealer_name, market, segment, sales_rep,
    account_status, created_date, updated_at, loaded_at
) VALUES (
    s.dealer_key, s.dealer_id, s.dealer_name, s.market, s.segment, s.sales_rep,
    s.account_status, s.created_date, s.updated_at, s.loaded_at
);
