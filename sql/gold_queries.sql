-- ============================================================================
-- Gold Layer Queries
-- ============================================================================
-- The Gold layer contains pre-computed business aggregates.
-- These queries are optimized for reporting and dashboarding.

-- ----------------------------------------------------------------------------
-- Main Gold Table: sales_by_category_region
-- ----------------------------------------------------------------------------

-- Show all data
SELECT * FROM nessie.gold.sales_by_category_region
ORDER BY total_sales DESC;

-- ----------------------------------------------------------------------------
-- Performance Analysis
-- ----------------------------------------------------------------------------

-- Top performing categories by sales
SELECT
    category,
    SUM(total_sales) AS global_sales,
    SUM(total_profit) AS global_profit,
    SUM(total_quantity) AS global_quantity,
    SUM(order_count) AS global_orders
FROM nessie.gold.sales_by_category_region
GROUP BY category
ORDER BY global_sales DESC;

-- Top performing regions
SELECT
    region,
    SUM(total_sales) AS regional_sales,
    SUM(total_profit) AS regional_profit,
    SUM(order_count) AS regional_orders,
    ROUND(100.0 * SUM(total_profit) / NULLIF(SUM(total_sales), 0), 2) AS profit_margin_pct
FROM nessie.gold.sales_by_category_region
GROUP BY region
ORDER BY regional_sales DESC;

-- Best combinations (category + region)
SELECT
    category,
    region,
    total_sales,
    total_profit,
    total_quantity,
    order_count,
    ROUND(100.0 * total_profit / total_sales, 2) AS profit_margin_pct
FROM nessie.gold.sales_by_category_region
ORDER BY total_sales DESC;

-- Most profitable combinations
SELECT
    category,
    region,
    total_sales,
    total_profit,
    total_quantity,
    order_count,
    ROUND(100.0 * total_profit / total_sales, 2) AS profit_margin_pct
FROM nessie.gold.sales_by_category_region
WHERE total_profit > 0
ORDER BY profit_margin_pct DESC;

-- Highest volume by quantity
SELECT
    category,
    region,
    total_sales,
    total_profit,
    total_quantity,
    order_count
FROM nessie.gold.sales_by_category_region
ORDER BY total_quantity DESC;

-- ----------------------------------------------------------------------------
-- Comparisons and Rankings
-- ----------------------------------------------------------------------------

-- Category performance within each region
SELECT
    region,
    category,
    total_sales,
    total_profit,
    profit_margin_pct,
    RANK() OVER (PARTITION BY region ORDER BY total_sales DESC) AS sales_rank_in_region
FROM (
    SELECT
        region,
        category,
        total_sales,
        total_profit,
        ROUND(100.0 * total_profit / total_sales, 2) AS profit_margin_pct
    FROM nessie.gold.sales_by_category_region
) t
ORDER BY region, sales_rank_in_region;

-- Region comparison for each category
SELECT
    category,
    region,
    total_sales,
    total_profit,
    total_sales - AVG(total_sales) OVER (PARTITION BY category) AS sales_vs_avg,
    RANK() OVER (PARTITION BY category ORDER BY total_sales DESC) AS sales_rank_in_category
FROM nessie.gold.sales_by_category_region
ORDER BY category, sales_rank_in_category;

-- ----------------------------------------------------------------------------
-- Summary Statistics
-- ----------------------------------------------------------------------------

-- Overall totals
SELECT
    SUM(total_sales) AS global_sales,
    SUM(total_profit) AS global_profit,
    SUM(total_quantity) AS global_quantity,
    SUM(order_count) AS global_orders,
    COUNT(*) AS category_region_combinations
FROM nessie.gold.sales_by_category_region;

-- Average per combination
SELECT
    ROUND(AVG(total_sales), 2) AS avg_sales,
    ROUND(AVG(total_profit), 2) AS avg_profit,
    ROUND(AVG(total_quantity), 2) AS avg_quantity,
    ROUND(AVG(order_count), 2) AS avg_orders,
    ROUND(MIN(total_sales), 2) AS min_sales,
    ROUND(MAX(total_sales), 2) AS max_sales
FROM nessie.gold.sales_by_category_region;

-- ----------------------------------------------------------------------------
-- Executive Dashboard Queries
-- ----------------------------------------------------------------------------

-- Key metrics summary
WITH overall AS (
    SELECT
        SUM(total_sales) AS global_sales,
        SUM(total_profit) AS global_profit,
        SUM(total_quantity) AS global_quantity,
        SUM(order_count) AS global_orders
    FROM nessie.gold.sales_by_category_region
),
top_category AS (
    SELECT category, SUM(total_sales) AS sales
    FROM nessie.gold.sales_by_category_region
    GROUP BY category
    ORDER BY sales DESC
    LIMIT 1
),
top_region AS (
    SELECT region, SUM(total_sales) AS sales
    FROM nessie.gold.sales_by_category_region
    GROUP BY region
    ORDER BY sales DESC
    LIMIT 1
)
SELECT
    o.global_sales,
    o.global_profit,
    ROUND(100.0 * o.global_profit / o.global_sales, 2) AS global_profit_margin_pct,
    o.global_quantity,
    o.global_orders,
    (SELECT category FROM top_category) AS top_category,
    (SELECT region FROM top_region) AS top_region
FROM overall o;
