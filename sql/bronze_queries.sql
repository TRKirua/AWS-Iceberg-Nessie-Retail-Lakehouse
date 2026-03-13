-- ============================================================================
-- Bronze Layer Queries
-- ============================================================================
-- The Bronze layer contains raw data from the source system.
-- Use these queries for exploration and data lineage tracking.

-- ----------------------------------------------------------------------------
-- Table Information
-- ----------------------------------------------------------------------------

-- Show table schema
SHOW CREATE TABLE nessie.bronze.sales;

-- Describe table columns
DESCRIBE nessie.bronze.sales;

-- ----------------------------------------------------------------------------
-- Data Exploration
-- ----------------------------------------------------------------------------

-- Count total records
SELECT COUNT(*) AS total_records FROM nessie.bronze.sales;

-- Sample raw data (first 10 rows)
SELECT * FROM nessie.bronze.sales LIMIT 10;

-- Show data by ingestion date
SELECT
    ingestion_date,
    COUNT(*) AS record_count,
    COUNT(DISTINCT batch_id) AS batch_count
FROM nessie.bronze.sales
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;

-- Show all source files
SELECT DISTINCT
    source_file,
    source_system,
    batch_id,
    MIN(ingestion_ts) AS first_ingestion,
    MAX(ingestion_ts) AS last_ingestion,
    COUNT(*) AS record_count
FROM nessie.bronze.sales
GROUP BY source_file, source_system, batch_id
ORDER BY first_ingestion DESC;

-- ----------------------------------------------------------------------------
-- Data Quality Checks (Bronze keeps everything)
-- ----------------------------------------------------------------------------

-- Check for NULL critical values (Bronze may have these)
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN `Order ID` IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN `Product ID` IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN Sales IS NULL THEN 1 ELSE 0 END) AS null_sales
FROM nessie.bronze.sales;

-- Check for potential duplicates by natural key
SELECT
    `Order ID`,
    `Product ID`,
    COUNT(*) AS occurrence_count
FROM nessie.bronze.sales
GROUP BY `Order ID`, `Product ID`
HAVING COUNT(*) > 1
LIMIT 10;

-- ----------------------------------------------------------------------------
-- Time Travel Queries (Iceberg feature)
-- ----------------------------------------------------------------------------

-- Query as of a specific snapshot ID
-- SELECT * FROM nessie.bronze.sales VERSION AS OF 'snapshot_id_here';

-- Query as of a specific timestamp
-- SELECT * FROM nessie.bronze.sales TIMESTAMP AS OF '2024-01-01 00:00:00';

-- ----------------------------------------------------------------------------
-- Partition Information
-- ----------------------------------------------------------------------------

-- Show partition values
SELECT
    ingestion_date,
    COUNT(*) AS record_count
FROM nessie.bronze.sales
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;

-- ----------------------------------------------------------------------------
-- Aggregations (for reference, usually done in Silver/Gold)
-- ----------------------------------------------------------------------------

-- Sales by category (raw names)
SELECT
    Category,
    ROUND(SUM(Sales), 2) AS total_sales,
    COUNT(*) AS record_count
FROM nessie.bronze.sales
GROUP BY Category
ORDER BY total_sales DESC;

-- Sales by region
SELECT
    Region,
    ROUND(SUM(Sales), 2) AS total_sales,
    ROUND(SUM(Profit), 2) AS total_profit,
    COUNT(*) AS record_count
FROM nessie.bronze.sales
GROUP BY Region
ORDER BY total_sales DESC;
