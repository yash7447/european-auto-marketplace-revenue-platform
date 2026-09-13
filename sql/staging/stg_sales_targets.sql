MERGE INTO auto_marketplace_staging.stg_sales_targets AS target
USING
(
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
        source_sha256,
        source_file,
        staged_at
    FROM
    (
        SELECT
            normalized.*,
            ROW_NUMBER() OVER
            (
                PARTITION BY target_id
                ORDER BY created_at DESC NULLS LAST,
                         planning_version DESC NULLS LAST,
                         source_sha256 DESC
            ) AS row_number
        FROM
        (
            SELECT
                CAST(target_id AS VARCHAR) AS target_id,
                TRY_CAST(target_month AS DATE) AS target_month,
                CAST(market AS VARCHAR) AS market,
                CAST(sales_rep AS VARCHAR) AS sales_rep,
                CAST(product_group AS VARCHAR) AS product_group,
                TRY_CAST(revenue_target_eur AS DECIMAL(14,2)) AS revenue_target_eur,
                TRY_CAST(upsell_target_eur AS DECIMAL(14,2)) AS upsell_target_eur,
                TRY_CAST(new_dealer_target AS INTEGER) AS new_dealer_target,
                TRY_CAST(retention_target_pct AS DECIMAL(7,4)) AS retention_target_pct,
                CAST(planning_version AS VARCHAR) AS planning_version,
                TRY_CAST(created_at AS TIMESTAMP) AS created_at,
                CAST(sha256 AS VARCHAR) AS source_sha256,
                "$path" AS source_file,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS staged_at
            FROM auto_marketplace_raw.raw_sales_targets
            WHERE
                __SOURCE_FILTER__
                AND "$path" LIKE '%sales_targets.csv'
        ) AS normalized
        WHERE target_id IS NOT NULL
    ) AS ranked
    WHERE row_number = 1
) AS source
ON target.target_id = source.target_id
WHEN MATCHED THEN
    UPDATE SET
        target_month = source.target_month,
        market = source.market,
        sales_rep = source.sales_rep,
        product_group = source.product_group,
        revenue_target_eur = source.revenue_target_eur,
        upsell_target_eur = source.upsell_target_eur,
        new_dealer_target = source.new_dealer_target,
        retention_target_pct = source.retention_target_pct,
        planning_version = source.planning_version,
        created_at = source.created_at,
        source_sha256 = source.source_sha256,
        source_file = source.source_file,
        staged_at = source.staged_at
WHEN NOT MATCHED THEN
    INSERT
    (
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
        source_sha256,
        source_file,
        staged_at
    )
    VALUES
    (
        source.target_id,
        source.target_month,
        source.market,
        source.sales_rep,
        source.product_group,
        source.revenue_target_eur,
        source.upsell_target_eur,
        source.new_dealer_target,
        source.retention_target_pct,
        source.planning_version,
        source.created_at,
        source.source_sha256,
        source.source_file,
        source.staged_at
    );
