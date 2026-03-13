-- ============================================================================
-- Silver Layer Queries
-- ============================================================================
-- The Silver layer contains standardized and cleaned data.
-- Use these queries for analytics and downstream processing.

-- ----------------------------------------------------------------------------
-- Table Information
-- ----------------------------------------------------------------------------

-- Show table schema
SHOW CREATE TABLE nessie.silver.sales;

-- Describe table columns
DESCRIBE nessie.silver.sales;

-- ----------------------------------------------------------------------------
-- Data Exploration
-- ----------------------------------------------------------------------------

-- Count total records
SELECT COUNT(*) AS total_records FROM nessie.silver.sales;

-- Sample standardized data
SELECT * FROM nessie.silver.sales LIMIT 10;

-- ----------------------------------------------------------------------------
-- Data Quality Verification
-- ----------------------------------------------------------------------------

-- Verify no NULL critical fields
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN sales IS NULL THEN 1 ELSE 0 END) AS null_sales
FROM nessie.silver.sales;

-- Check for invalid values
SELECT
    SUM(CASE WHEN sales < 0 THEN 1 ELSE 0 END) AS negative_sales,
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS invalid_quantity,
    SUM(CASE WHEN discount < 0 OR discount > 1 THEN 1 ELSE 0 END) AS invalid_discount
FROM nessie.silver.sales;

-- ----------------------------------------------------------------------------
-- Business Analytics
-- ----------------------------------------------------------------------------

-- Sales by category
SELECT
    category,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT order_id) AS order_count
FROM nessie.silver.sales
GROUP BY category
ORDER BY total_sales DESC;

-- Sales by region
SELECT
    region,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT order_id) AS order_count
FROM nessie.silver.sales
GROUP BY region
ORDER BY total_sales DESC;

-- Sales by segment
SELECT
    segment,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(AVG(sales), 2) AS avg_order_value
FROM nessie.silver.sales
GROUP BY segment
ORDER BY total_sales DESC;

-- Top performing products
SELECT
    category,
    sub_category,
    product_name,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    SUM(quantity) AS total_quantity
FROM nessie.silver.sales
GROUP BY category, sub_category, product_name
ORDER BY total_sales DESC
LIMIT 20;

-- Top customers by sales
SELECT
    customer_id,
    customer_name,
    segment,
    region,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit
FROM nessie.silver.sales
GROUP BY customer_id, customer_name, segment, region
ORDER BY total_sales DESC
LIMIT 20;

-- Monthly sales trend
SELECT
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    COUNT(DISTINCT order_id) AS order_count
FROM nessie.silver.sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- ----------------------------------------------------------------------------
-- Filter Queries
-- ----------------------------------------------------------------------------

-- High value orders (sales > 1000)
SELECT
    order_id,
    order_date,
    customer_name,
    category,
    product_name,
    sales,
    profit
FROM nessie.silver.sales
WHERE sales > 1000
ORDER BY sales DESC;

-- Unprofitable transactions
SELECT
    order_id,
    order_date,
    category,
    product_name,
    sales,
    profit,
    discount
FROM nessie.silver.sales
WHERE profit < 0
ORDER BY profit ASC
LIMIT 20;

-- Large discounts
SELECT
    order_id,
    order_date,
    category,
    product_name,
    sales,
    discount,
    quantity
FROM nessie.silver.sales
WHERE discount > 0.5
ORDER BY discount DESC
LIMIT 20;
