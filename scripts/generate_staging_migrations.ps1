$ErrorActionPreference = "Stop"


# ============================================================
# PATHS
# ============================================================

$RepoRoot = Split-Path -Parent $PSScriptRoot

$MigrationDir = Join-Path `
    $RepoRoot `
    "sql\migrations"

if (-not (Test-Path $MigrationDir)) {
    throw "Migration directory does not exist: $MigrationDir"
}


# ============================================================
# UTF-8 WITHOUT BOM
# ============================================================

$Utf8NoBom = New-Object `
    System.Text.UTF8Encoding($false)


# ============================================================
# NORMALIZE CONTENT
#
# We force LF line endings so Windows and Jenkins/Linux
# generate identical migration contents.
# ============================================================

function Normalize-Text {

    param(
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    $Normalized = $Text `
        -replace "`r`n", "`n" `
        -replace "`r", "`n"

    return $Normalized.Trim() + "`n"
}


# ============================================================
# STAGING MIGRATIONS
# ============================================================

$Migrations = [ordered]@{


# ============================================================
# V013 - MARKETPLACE LISTINGS
# ============================================================

"V013__create_stg_marketplace_listings.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_marketplace_listings
(
    listing_id string,
    vehicle_id string,
    dealer_id string,

    listing_start_date date,
    listing_end_date date,

    asking_price_eur decimal(12,2),

    listing_status string,
    visibility_package string,

    created_at timestamp,
    updated_at timestamp,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    month(listing_start_date)
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/marketplace/listings/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V014 - MARKETPLACE VEHICLES
# ============================================================

"V014__create_stg_marketplace_vehicles.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_marketplace_vehicles
(
    vehicle_id string,

    make string,
    model string,

    model_year int,

    fuel_type string,
    body_type string,

    mileage_km int,

    created_at timestamp,
    updated_at timestamp,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/marketplace/vehicles/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V015 - COMMERCIAL ACCOUNTS
# ============================================================

"V015__create_stg_commercial_accounts.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_accounts
(
    dealer_id string,
    dealer_name string,

    market string,
    segment string,

    sales_rep string,

    account_status string,

    created_date date,
    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/accounts/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V016 - COMMERCIAL CONTRACTS
# ============================================================

"V016__create_stg_commercial_contracts.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_contracts
(
    contract_id string,
    dealer_id string,

    contract_start_date date,
    contract_end_date date,

    contract_status string,

    auto_renew boolean,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/contracts/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V017 - COMMERCIAL SUBSCRIPTIONS
# ============================================================

"V017__create_stg_commercial_subscriptions.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_subscriptions
(
    subscription_id string,
    contract_id string,
    dealer_id string,

    product_id string,

    subscription_start_date date,
    subscription_end_date date,

    contract_price decimal(12,2),
    discount_pct decimal(7,4),

    billing_frequency string,
    subscription_status string,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    month(subscription_start_date)
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/subscriptions/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V018 - COMMERCIAL INVOICES
# ============================================================

"V018__create_stg_commercial_invoices.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_commercial_invoices
(
    invoice_id string,
    subscription_id string,
    dealer_id string,

    invoice_date date,

    invoice_amount decimal(14,2),

    currency string,
    payment_status string,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    month(invoice_date)
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/commercial/invoices/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V019 - PRICING PRODUCTS
# ============================================================

"V019__create_stg_pricing_products.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_pricing_products
(
    product_id string,
    product_name string,
    product_family string,

    billing_frequency string,

    active boolean,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/pricing/products/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V020 - PRICING PRICES
# ============================================================

"V020__create_stg_pricing_prices.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_pricing_prices
(
    price_id string,
    product_id string,

    market string,
    dealer_segment string,

    currency string,

    list_price decimal(12,2),

    effective_from date,
    effective_to date,

    pricing_version string,

    updated_at date,

    source_extract_date date,
    source_run_id string,
    source_file string,

    staged_at timestamp
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/pricing/prices/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V021 - ENGAGEMENT EVENTS
# ============================================================

"V021__create_stg_engagement_events.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_engagement_events
(
    event_id string,

    event_timestamp timestamp,
    event_date date,

    listing_id string,
    dealer_id string,
    session_id string,

    event_type string,
    device string,

    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    event_date
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/engagement/events/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@


# ============================================================
# V022 - SALES TARGETS
# ============================================================

"V022__create_stg_sales_targets.sql" = @'

CREATE TABLE IF NOT EXISTS
auto_marketplace_staging.stg_sales_targets
(
    target_id string,

    target_month date,

    market string,
    sales_rep string,
    product_group string,

    revenue_target_eur decimal(14,2),
    upsell_target_eur decimal(14,2),

    new_dealer_target int,
    retention_target_pct decimal(7,4),

    planning_version string,

    created_at timestamp,

    source_sha256 string,
    source_file string,

    staged_at timestamp
)
PARTITIONED BY (
    target_month
)
LOCATION
's3://european-auto-marketplace-data-yash-2026-01/staging/sales_targets/'
TBLPROPERTIES
(
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

'@

}


# ============================================================
# WRITE MIGRATIONS
#
# IMPORTANT:
#
# Existing migration files are NEVER silently overwritten.
#
# If the existing file is byte/content-equivalent after
# line-ending normalization, it is left alone.
#
# If the content differs, execution stops because an applied
# migration must never be modified.
# ============================================================

Write-Host ""
Write-Host "========================================"
Write-Host "GENERATING STAGING MIGRATIONS"
Write-Host "========================================"
Write-Host ""


$Created = 0
$Unchanged = 0


foreach ($Entry in $Migrations.GetEnumerator()) {

    $FileName = $Entry.Key

    $Path = Join-Path `
        $MigrationDir `
        $FileName

    $Expected = Normalize-Text `
        $Entry.Value


    if (Test-Path $Path) {

        $Existing = Normalize-Text `
            ([System.IO.File]::ReadAllText($Path))

        if ($Existing -ne $Expected) {

            Write-Host ""
            Write-Host "CONFLICT:"
            Write-Host $FileName
            Write-Host ""

            throw @"
Migration already exists with different content.

File:
$Path

Do NOT overwrite an existing migration.

If this migration has already been applied, create a new
migration version instead.
"@
        }


        Write-Host "UNCHANGED  $FileName"

        $Unchanged++

        continue
    }


    [System.IO.File]::WriteAllText(
        $Path,
        $Expected,
        $Utf8NoBom
    )


    Write-Host "CREATED    $FileName"

    $Created++
}


Write-Host ""
Write-Host "========================================"
Write-Host "GENERATION COMPLETE"
Write-Host "========================================"

Write-Host "Created:   $Created"
Write-Host "Unchanged: $Unchanged"
Write-Host "Total:     $($Migrations.Count)"

Write-Host ""
Write-Host "Migration directory:"
Write-Host $MigrationDir
Write-Host ""