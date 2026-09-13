MERGE INTO auto_marketplace_intermediate.int_dealer_subscription_snapshot t
USING (
    SELECT
        s.subscription_id,
        s.contract_id,
        s.dealer_id,
        a.dealer_name,
        a.market,
        a.segment,
        a.sales_rep,
        a.account_status,
        s.product_id,
        p.product_name,
        p.product_family,
        s.billing_frequency,
        s.subscription_start_date,
        s.subscription_end_date,
        c.contract_start_date,
        c.contract_end_date,
        s.contract_price,
        s.discount_pct,
        s.subscription_status,
        c.contract_status,
        c.auto_renew,
        a.updated_at AS account_updated_at,
        s.updated_at AS subscription_updated_at,
        c.updated_at AS contract_updated_at,
        p.updated_at AS product_updated_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at
    FROM auto_marketplace_staging.stg_commercial_subscriptions s
    LEFT JOIN auto_marketplace_staging.stg_commercial_contracts c
        ON s.contract_id = c.contract_id
    LEFT JOIN auto_marketplace_staging.stg_commercial_accounts a
        ON s.dealer_id = a.dealer_id
    LEFT JOIN auto_marketplace_staging.stg_pricing_products p
        ON s.product_id = p.product_id
) s
ON t.subscription_id = s.subscription_id
WHEN MATCHED THEN UPDATE SET
    contract_id=s.contract_id,
    dealer_id=s.dealer_id,
    dealer_name=s.dealer_name,
    market=s.market,
    segment=s.segment,
    sales_rep=s.sales_rep,
    account_status=s.account_status,
    product_id=s.product_id,
    product_name=s.product_name,
    product_family=s.product_family,
    billing_frequency=s.billing_frequency,
    subscription_start_date=s.subscription_start_date,
    subscription_end_date=s.subscription_end_date,
    contract_start_date=s.contract_start_date,
    contract_end_date=s.contract_end_date,
    contract_price=s.contract_price,
    discount_pct=s.discount_pct,
    subscription_status=s.subscription_status,
    contract_status=s.contract_status,
    auto_renew=s.auto_renew,
    account_updated_at=s.account_updated_at,
    subscription_updated_at=s.subscription_updated_at,
    contract_updated_at=s.contract_updated_at,
    product_updated_at=s.product_updated_at,
    transformed_at=s.transformed_at
WHEN NOT MATCHED THEN INSERT (
    subscription_id, contract_id, dealer_id, dealer_name, market, segment,
    sales_rep, account_status, product_id, product_name, product_family,
    billing_frequency, subscription_start_date, subscription_end_date,
    contract_start_date, contract_end_date, contract_price, discount_pct,
    subscription_status, contract_status, auto_renew, account_updated_at,
    subscription_updated_at, contract_updated_at, product_updated_at,
    transformed_at
) VALUES (
    s.subscription_id, s.contract_id, s.dealer_id, s.dealer_name, s.market,
    s.segment, s.sales_rep, s.account_status, s.product_id, s.product_name,
    s.product_family, s.billing_frequency, s.subscription_start_date,
    s.subscription_end_date, s.contract_start_date, s.contract_end_date,
    s.contract_price, s.discount_pct, s.subscription_status, s.contract_status,
    s.auto_renew, s.account_updated_at, s.subscription_updated_at,
    s.contract_updated_at, s.product_updated_at, s.transformed_at
);
