-- =============================================================================
-- transform.sql — Post-load SQL transformations for ELT example 18
-- =============================================================================
-- Run AFTER the flogo-datapipeline has loaded raw_sales_staging.
-- Execute with:  sqlite3 ./data/analytics.db < transform.sql
-- Or run:        ./post-transform.sh
--
-- This script does what an ETL pipeline transform chain would normally handle,
-- but instead it runs INSIDE the database.  This is the core ELT pattern:
--   - The pipeline (flogo-datapipeline) is responsible only for reliable,
--     fast extraction and loading of raw data.
--   - The database (or a tool like dbt) is responsible for all business logic,
--     normalisation, aggregation, and reporting table construction.
-- =============================================================================

-- -----------------------------------------------------------------------
-- STEP 1 — Normalise region codes to upper case.
--           Source data has mixed case: 'emea', 'amer', 'apac'.
--           Target always stores upper case: 'EMEA', 'AMER', 'APAC'.
-- -----------------------------------------------------------------------
-- (Applied in the clean_sales CTE below; no separate UPDATE needed.)

-- -----------------------------------------------------------------------
-- STEP 2 — Normalise raw_category to a canonical category name.
--           Source has: 'Electronics', 'ELECTRONICS', 'electronics',
--           'Clothing', 'CLOTHING', 'clothing',
--           'Food & Bev', 'food and bev', 'food and beverage',
--           'Food & Beverage'
--           Canonical values: 'Electronics', 'Clothing', 'Food & Beverage'
-- -----------------------------------------------------------------------
-- (Applied in the clean_sales CTE via CASE expression.)

-- -----------------------------------------------------------------------
-- STEP 3 — Convert all amounts to USD using approximate FX rates.
--           Rates (approximate for 2024-Q1):
--             EUR → USD : 1.09
--             GBP → USD : 1.27
--             JPY → USD : 0.0067
--             INR → USD : 0.012
--             AUD → USD : 0.65
--             USD → USD : 1.00
-- -----------------------------------------------------------------------
-- (Applied in the clean_sales CTE via CASE expression on currency.)

-- -----------------------------------------------------------------------
-- DROP old analytics tables so this script is idempotent (re-runnable).
-- -----------------------------------------------------------------------
DROP TABLE IF EXISTS clean_sales;
DROP TABLE IF EXISTS sales_by_region;
DROP TABLE IF EXISTS sales_by_category;
DROP TABLE IF EXISTS sales_by_region_category;
DROP TABLE IF EXISTS daily_revenue;

-- -----------------------------------------------------------------------
-- STEP 4 — Build clean_sales: normalised, USD-converted fact table.
--           This is the single "trusted" table that all reports query.
-- -----------------------------------------------------------------------
CREATE TABLE clean_sales AS
SELECT
    id,
    UPPER(region)  AS region,

    -- Canonical category: collapse all spelling variants to three values.
    CASE
        WHEN LOWER(raw_category) IN ('electronics', 'electronic')
            THEN 'Electronics'
        WHEN LOWER(raw_category) IN ('clothing', 'clothes')
            THEN 'Clothing'
        WHEN LOWER(raw_category) IN (
                'food & bev', 'food and bev',
                'food & beverage', 'food and beverage',
                'food & beverages')
            THEN 'Food & Beverage'
        ELSE raw_category   -- preserve unexpected values for inspection
    END AS category,

    amount          AS raw_amount,
    currency        AS raw_currency,

    -- Convert to USD.
    ROUND(
        amount * CASE currency
            WHEN 'USD' THEN 1.0000
            WHEN 'EUR' THEN 1.0900
            WHEN 'GBP' THEN 1.2700
            WHEN 'JPY' THEN 0.0067
            WHEN 'INR' THEN 0.0120
            WHEN 'AUD' THEN 0.6500
            ELSE 1.0   -- unknown currency: keep as-is, flag via raw_currency
        END,
        2
    ) AS amount_usd,

    sale_date,
    raw_category    -- retain original for audit / lineage
FROM raw_sales_staging;

-- -----------------------------------------------------------------------
-- STEP 5 — Aggregate: total revenue and deal count per region.
-- -----------------------------------------------------------------------
CREATE TABLE sales_by_region AS
SELECT
    region,
    COUNT(*)                        AS deal_count,
    ROUND(SUM(amount_usd), 2)       AS total_revenue_usd,
    ROUND(AVG(amount_usd), 2)       AS avg_deal_usd,
    ROUND(MIN(amount_usd), 2)       AS min_deal_usd,
    ROUND(MAX(amount_usd), 2)       AS max_deal_usd
FROM clean_sales
GROUP BY region
ORDER BY total_revenue_usd DESC;

-- -----------------------------------------------------------------------
-- STEP 6 — Aggregate: total revenue and deal count per canonical category.
-- -----------------------------------------------------------------------
CREATE TABLE sales_by_category AS
SELECT
    category,
    COUNT(*)                        AS deal_count,
    ROUND(SUM(amount_usd), 2)       AS total_revenue_usd,
    ROUND(AVG(amount_usd), 2)       AS avg_deal_usd
FROM clean_sales
GROUP BY category
ORDER BY total_revenue_usd DESC;

-- -----------------------------------------------------------------------
-- STEP 7 — Aggregate: region × category cross-tab (pivot-style summary).
-- -----------------------------------------------------------------------
CREATE TABLE sales_by_region_category AS
SELECT
    region,
    category,
    COUNT(*)                        AS deal_count,
    ROUND(SUM(amount_usd), 2)       AS total_revenue_usd
FROM clean_sales
GROUP BY region, category
ORDER BY region, total_revenue_usd DESC;

-- -----------------------------------------------------------------------
-- STEP 8 — Daily revenue time series (for trend/chart queries).
-- -----------------------------------------------------------------------
CREATE TABLE daily_revenue AS
SELECT
    sale_date,
    COUNT(*)                        AS deals,
    ROUND(SUM(amount_usd), 2)       AS revenue_usd
FROM clean_sales
GROUP BY sale_date
ORDER BY sale_date;

-- -----------------------------------------------------------------------
-- SUMMARY — print row counts so the post-transform.sh can confirm output.
-- -----------------------------------------------------------------------
SELECT '=== Transform complete ===' AS status;
SELECT 'clean_sales rows:             ' || COUNT(*) FROM clean_sales;
SELECT 'sales_by_region rows:         ' || COUNT(*) FROM sales_by_region;
SELECT 'sales_by_category rows:       ' || COUNT(*) FROM sales_by_category;
SELECT 'sales_by_region_category rows:' || COUNT(*) FROM sales_by_region_category;
SELECT 'daily_revenue rows:           ' || COUNT(*) FROM daily_revenue;
