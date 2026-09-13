MERGE INTO auto_marketplace_intermediate.int_invoice_enriched t
USING (
    SELECT
        i.invoice_id,
        i.invoice_date,
        i.invoice_amount,
        i.currency,
        i.payment_status,
        i.subscription_id,
        s.contract_id,
        i.dealer_id,
        a.dealer_name,
        a.market,
        a.segment,
        a.sales_rep,
        s.product_id,
        p.product_name,
        p.product_family,
        s.billing_frequency,
        s.contract_price,
        s.discount_pct,
        s.subscription_status,
        c.contract_status,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at
    FROM auto_marketplace_staging.stg_commercial_invoices i
    LEFT JOIN auto_marketplace_staging.stg_commercial_subscriptions s
        ON i.subscription_id = s.subscription_id
    LEFT JOIN auto_marketplace_staging.stg_commercial_contracts c
        ON s.contract_id = c.contract_id
    LEFT JOIN auto_marketplace_staging.stg_commercial_accounts a
        ON i.dealer_id = a.dealer_id
    LEFT JOIN auto_marketplace_staging.stg_pricing_products p
        ON s.product_id = p.product_id
) s
ON t.invoice_id = s.invoice_id
WHEN MATCHED THEN UPDATE SET
    invoice_date=s.invoice_date,
    invoice_amount=s.invoice_amount,
    currency=s.currency,
    payment_status=s.payment_status,
    subscription_id=s.subscription_id,
    contract_id=s.contract_id,
    dealer_id=s.dealer_id,
    dealer_name=s.dealer_name,
    market=s.market,
    segment=s.segment,
    sales_rep=s.sales_rep,
    product_id=s.product_id,
    product_name=s.product_name,
    product_family=s.product_family,
    billing_frequency=s.billing_frequency,
    contract_price=s.contract_price,
    discount_pct=s.discount_pct,
    subscription_status=s.subscription_status,
    contract_status=s.contract_status,
    transformed_at=s.transformed_at
WHEN NOT MATCHED THEN INSERT (
    invoice_id, invoice_date, invoice_amount, currency, payment_status,
    subscription_id, contract_id, dealer_id, dealer_name, market, segment,
    sales_rep, product_id, product_name, product_family, billing_frequency,
    contract_price, discount_pct, subscription_status, contract_status,
    transformed_at
) VALUES (
    s.invoice_id, s.invoice_date, s.invoice_amount, s.currency, s.payment_status,
    s.subscription_id, s.contract_id, s.dealer_id, s.dealer_name, s.market,
    s.segment, s.sales_rep, s.product_id, s.product_name, s.product_family,
    s.billing_frequency, s.contract_price, s.discount_pct, s.subscription_status,
    s.contract_status, s.transformed_at
);
