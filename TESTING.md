# Testing Guide

This guide explains how to test and verify the lakehouse pipeline end-to-end.

## Prerequisites Checklist

Before running tests, ensure:

- [ ] **Nessie is running**: `docker run -p 19120:19120 ghcr.io/projectnessie/nessie:latest`
- [ ] **AWS credentials** are configured in `.env`
- [ ] **Python dependencies** are installed: `pip install -r requirements.txt`
- [ ] **S3 bucket exists** and is accessible

Verify Nessie:
```bash
curl http://localhost:19120/api/v2/config
```

Verify AWS:
```bash
python scripts/test_aws_connection.py
```

---

## Test 1: Single File Pipeline (Quick Start)

This test runs the complete pipeline with a single source file.

### Step 1: Upload Source to S3

```bash
python scripts/upload_to_s3.py --list
```

### Step 2: Run Full Pipeline

```bash
python scripts/run_full_pipeline.py --drop-all
```

### Step 3: Verify Results

```bash
# Check Bronze count
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()
print('Bronze:', spark.sql('SELECT COUNT(*) FROM nessie.bronze.sales').first()[0])
print('Silver:', spark.sql('SELECT COUNT(*) FROM nessie.silver.sales').first()[0])
print('Gold:', spark.sql('SELECT COUNT(*) FROM nessie.gold.sales_by_category_region').first()[0])
"
```

**Expected Output:**
- Bronze: ~9,994 records
- Silver: ~9,986 records (13 records filtered)
- Gold: 12 records (aggregated)

### Step 4: Check Iceberg Versioning

```bash
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()
spark.sql('SELECT * FROM nessie.bronze.sales.history').show(truncate=False)
"
```

---

## Test 2: Multi-Batch Pipeline (Quality Demo)

This test demonstrates data quality filtering with multiple batch files containing intentional issues.

### Step 1: Generate Test Data with Quality Issues

```bash
python scripts/generate_batch_data.py --num-batches 3
```

This creates 3 files in `data/raw_batches/` with:
- Null Sales values
- Null Product IDs
- Invalid discounts (> 1 or < 0)
- Invalid quantities (<= 0)
- Duplicate records

### Step 2: Ingest Batches to Bronze

```bash
python scripts/ingest_multi_batch.py --drop-first
```

**Expected behavior:**
- All records (including invalid ones) are kept in Bronze
- 3 batch files are processed sequentially
- Each batch gets a unique `batch_id`

### Step 3: Run Silver Transformations

```bash
python scripts/create_silver_tables.py --drop-first
```

**Expected behavior:**
- Records with null `order_id`, `product_id`, or `sales` are filtered
- Duplicate `(order_id, product_id)` combinations are removed
- Text fields are trimmed
- Column names are standardized to snake_case

### Step 4: Run Gold Aggregations

```bash
python scripts/create_gold_tables.py --drop-first
```

### Step 5: Quality Report

```bash
python -c "
from lakehouse.spark_session import get_spark
from lakehouse.silver import run_quality_checks

spark = get_spark()
checks = run_quality_checks(spark, 'nessie.silver.sales')

print('=== Quality Report ===')
print(f'Total records: {checks[\"total_records\"]}')
print(f'Null critical fields: {sum(checks[\"null_checks\"].values())}')
print(f'Negative sales: {checks[\"negative_sales\"]}')
print(f'Invalid quantity: {checks[\"invalid_quantity\"]}')
print(f'Invalid discount: {checks[\"invalid_discount\"]}')
"
```

**Expected:**
- Null critical fields: 0
- Negative sales: 0
- Invalid quantity: 0
- Invalid discount: 0

---

## Test 3: Time Travel (Iceberg Feature)

Demonstrate Iceberg's time travel capabilities.

### Step 1: Create Multiple Snapshots

```bash
# First ingestion
python scripts/create_bronze_tables.py --drop-first

# Second ingestion (append)
python scripts/create_bronze_tables.py --append
```

### Step 2: Query History

```bash
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()

print('=== Snapshot History ===')
spark.sql('SELECT * FROM nessie.bronze.sales.history').show(truncate=False)

print('\\n=== Snapshot Details ===')
spark.sql('SELECT snapshot_id, operation, committed_at FROM nessie.bronze.sales.snapshots').show(truncate=False)
"
```

### Step 3: Query Previous Snapshot

```bash
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()

# Get snapshot IDs
snapshots = spark.sql('SELECT snapshot_id FROM nessie.bronze.sales.snapshots ORDER BY committed_at').collect()
first_snapshot = snapshots[0][0]

print(f'=== Querying snapshot: {first_snapshot} ===')
# Query using VERSION AS OF (replace with actual snapshot ID)
# spark.sql(f\"SELECT * FROM nessie.bronze.sales VERSION AS OF '{first_snapshot}'\").show()
"
```

---

## Test 4: Layer-by-Layer Validation

Validate each layer independently.

### Bronze Layer Validation

```bash
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()

# Check table exists
print('Tables in bronze namespace:')
spark.sql('SHOW TABLES IN nessie.bronze').show()

# Check partitioning
print('\\nPartitions:')
spark.sql('SELECT ingestion_date, COUNT(*) FROM nessie.bronze.sales GROUP BY ingestion_date').show()

# Check metadata columns
print('\\nMetadata columns:')
spark.sql('SELECT source_file, batch_id, COUNT(*) FROM nessie.bronze.sales GROUP BY source_file, batch_id').show()
"
```

### Silver Layer Validation

```bash
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()

# Check schema
print('Silver schema:')
spark.sql('DESCRIBE nessie.silver.sales').show()

# Check data types
print('\\nSample data:')
spark.sql('SELECT * FROM nessie.silver.sales LIMIT 5').show()

# Check for PascalCase remnants (should be none)
print('\\nChecking for non-standard columns...')
"
```

### Gold Layer Validation

```bash
python -c "
from lakehouse.spark_session import get_spark
spark = get_spark()

# Show all aggregates
print('Gold aggregates:')
spark.sql('SELECT * FROM nessie.gold.sales_by_category_region ORDER BY total_sales DESC').show()

# Show profit margins
print('\\nProfit margins:')
spark.sql('SELECT category, region, ROUND(100.0 * total_profit / total_sales, 2) AS margin_pct FROM nessie.gold.sales_by_category_region ORDER BY margin_pct DESC').show()
"
```

---

## Test 5: Notebook Execution

Run notebooks sequentially for interactive testing.

```bash
# Start Jupyter
jupyter lab

# Or use VS Code:
code notebooks/01_setup_environment.ipynb
```

Execute cells in order:
1. `01_setup_environment.ipynb` - Create namespaces
2. `02_ingestion_bronze.ipynb` - Bronze ingestion
3. `03_silver_transformations.ipynb` - Silver transformations
4. `04_gold_layer.ipynb` - Gold aggregations

---

## Troubleshooting Tests

### Nessie Connection Failed

```bash
# Check if Nessie is running
curl http://localhost:19120/api/v2/config

# Restart Nessie
docker run -p 19120:19120 ghcr.io/projectnessie/nessie:latest
```

### Table Already Exists

```bash
# Drop and recreate
python scripts/run_full_pipeline.py --drop-all
```

### S3 Access Denied

```bash
# Test AWS connection
python scripts/test_aws_connection.py

# Verify .env credentials
cat .env | grep AWS
```

### Out of Memory

```bash
# Increase Spark memory
export PYSPARK_DRIVER_MEMORY=4g
python scripts/run_full_pipeline.py
```

---

## Test Summary Checklist

Run this checklist to verify the entire pipeline:

- [ ] Nessie is accessible
- [ ] AWS credentials work
- [ ] Bronze table created with correct count
- [ ] Bronze has metadata columns (ingestion_date, batch_id, etc.)
- [ ] Bronze snapshot history shows multiple entries
- [ ] Silver table created with fewer records (filtering worked)
- [ ] Silver has snake_case column names
- [ ] Silver has no nulls on critical fields
- [ ] Gold table created with 12 records
- [ ] Gold aggregates are correctly calculated
- [ ] Time travel queries work on history table
