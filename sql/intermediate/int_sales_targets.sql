MERGE INTO auto_marketplace_intermediate.int_sales_targets t
USING (
    SELECT
        target_id,
        target_month,
        market,
        sales_rep,
        product_group,
        revenue_target_eur,
        upsell_target_eur,
        new_dealer_target,
        retention_target_pct,
        planning_version,
        created_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS transformed_at
    FROM auto_marketplace_staging.stg_sales_targets
) s
ON t.target_id = s.target_id
WHEN MATCHED THEN UPDATE SET
    target_month=s.target_month,
    market=s.market,
    sales_rep=s.sales_rep,
    product_group=s.product_group,
    revenue_target_eur=s.revenue_target_eur,
    upsell_target_eur=s.upsell_target_eur,
    new_dealer_target=s.new_dealer_target,
    retention_target_pct=s.retention_target_pct,
    planning_version=s.planning_version,
    created_at=s.created_at,
    transformed_at=s.transformed_at
WHEN NOT MATCHED THEN INSERT (
    target_id, target_month, market, sales_rep, product_group,
    revenue_target_eur, upsell_target_eur, new_dealer_target,
    retention_target_pct, planning_version, created_at, transformed_at
) VALUES (
    s.target_id, s.target_month, s.market, s.sales_rep, s.product_group,
    s.revenue_target_eur, s.upsell_target_eur, s.new_dealer_target,
    s.retention_target_pct, s.planning_version, s.created_at, s.transformed_at
);
