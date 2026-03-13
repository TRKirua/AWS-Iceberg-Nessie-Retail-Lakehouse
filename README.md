# Versioned Retail Lakehouse on AWS with Apache Iceberg & Nessie

A professional demonstration of a modern data lakehouse architecture using:
- **AWS S3** for object storage
- **Apache Iceberg** for ACID table format with time travel
- **Project Nessie** for Git-like data catalog versioning
- **Apache Spark** for data processing
- **Bronze / Silver / Gold** medallion architecture

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA LAKEHOUSE                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   CSV Source ──► S3 Raw Zone ──► Bronze ──► Silver ──► Gold             │
│                                 (Iceberg)  (Iceberg) (Iceberg)          │
│                                     │          │         │              │
│                                     ▼          ▼         ▼              │
│                                   Nessie Catalog (Versioned)            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer      | Purpose             | Characteristics                                                       |
|------------|---------------------|-----------------------------------------------------------------------|
| **Bronze** | Raw data landing    | Append-only, partitioned by `ingestion_date`, preserves source format |
| **Silver** | Standardization     | Column renaming, quality filters, deduplication, type casting         |
| **Gold**   | Business aggregates | Pre-computed metrics, rounded values, reporting-ready                 |

## Project Structure

```
aws-iceberg-nessie-retail-lakehouse/
├── .github/
│   └── workflows/
│       └── data_pipeline.yml      # CI/CD pipeline validation
├── data/
│   ├── raw/                       # Single source file
│   │   └── superstore_sales.csv
│   └── raw_batches/               # Multi-batch generated files
├── notebooks/                     # Jupyter notebooks for demo
│   ├── 01_setup_environment.ipynb
│   ├── 02_ingestion_bronze.ipynb
│   ├── 03_silver_transformations.ipynb
│   └── 04_gold_layer.ipynb
├── scripts/                       # Executable Python scripts
│   ├── create_bronze_tables.py
│   ├── create_silver_tables.py
│   ├── create_gold_tables.py
│   ├── run_full_pipeline.py
│   ├── ingest_multi_batch.py
│   ├── generate_batch_data.py
│   └── upload_to_s3.py
├── sql/                           # Useful query templates
│   ├── bronze_queries.sql
│   ├── silver_queries.sql
│   ├── gold_queries.sql
│   └── iceberg_metadata_queries.sql
└── src/lakehouse/                 # Python modules
    ├── __init__.py
    ├── settings.py                # Configuration management
    ├── spark_session.py           # Spark session builder
    ├── bronze.py                  # Bronze layer logic
    ├── silver.py                  # Silver layer logic
    └── gold.py                    # Gold layer logic
```

## Prerequisites

### Software Required
- Python 3.11+
- Java 11+ (for Spark)
- Apache Spark 3.5.1 (managed via pyspark)
- Docker Desktop (for Nessie)

### AWS Resources
- S3 bucket
- AWS credentials with S3 access

### Environment Variables

Create a `.env` file in the project root:

```bash
# AWS Configuration
AWS_REGION=eu-west-3
AWS_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key

# Nessie Configuration
NESSIE_URI=http://localhost:19120/api/v2
NESSIE_BRANCH=main

# Warehouse Location
WAREHOUSE=s3://your-bucket-name/
```

## Quick Start

### 1. Start Nessie Catalog

```bash
docker run -p 19120:19120 ghcr.io/projectnessie/nessie:latest
```

Verify Nessie is running:
```bash
curl http://localhost:19120/api/v2/config
```

### 2. Upload Source Data to S3

```bash
python scripts/upload_to_s3.py
```

### 3. Run the Complete Pipeline

```bash
# Single file ingestion
python scripts/run_full_pipeline.py

# Or with options
python scripts/run_full_pipeline.py --drop-all
```

### 4. Verify Results

```bash
# Check Bronze
python -c "from lakehouse.spark_session import get_spark; spark = get_spark(); spark.sql('SELECT COUNT(*) FROM nessie.bronze.sales').show()"

# Check Silver
python -c "from lakehouse.spark_session import get_spark; spark = get_spark(); spark.sql('SELECT COUNT(*) FROM nessie.silver.sales').show()"

# Check Gold
python -c "from lakehouse.spark_session import get_spark; spark = get_spark(); spark.sql('SELECT * FROM nessie.gold.sales_by_category_region ORDER BY total_sales DESC').show()"
```

## Multi-Batch Ingestion Demo

### Generate Batch Files with Quality Issues

```bash
# Generate 3 batch files with ~5% data quality issues
python scripts/generate_batch_data.py --num-batches 3
```

This creates files with intentional issues:
- Null sales values
- Null product IDs
- Invalid discounts (> 1 or < 0)
- Invalid quantities (<= 0)
- Duplicate records

### Ingest Multiple Batches

```bash
# Process all batch files
python scripts/ingest_multi_batch.py --drop-first
```

## Available Scripts

| Script                    | Purpose                    | Key Options                     |
|---------------------------|----------------------------|---------------------------------|
| `run_full_pipeline.py`    | Run Bronze → Silver → Gold | `--drop-all`, `--append-bronze` |
| `create_bronze_tables.py` | Bronze layer only          | `--append`, `--drop-first`      |
| `create_silver_tables.py` | Silver layer only          | `--drop-first`                  |
| `create_gold_tables.py`   | Gold layer only            | `--drop-first`                  |
| `ingest_multi_batch.py`   | Multi-batch ingestion      | `--s3-mode`, `--drop-first`     |
| `generate_batch_data.py`  | Generate test batches      | `--num-batches`, `--issue-rate` |
| `upload_to_s3.py`         | Upload data to S3          | -                               |

## Iceberg & Nessie Features

### Time Travel

Query previous snapshots:

```sql
-- Use snapshot ID
SELECT * FROM nessie.bronze.sales VERSION AS OF 'snapshot_id'

-- Use timestamp
SELECT * FROM nessie.bronze.sales TIMESTAMP AS OF '2024-01-01 00:00:00'
```

### View History

```sql
-- Snapshot history
SELECT * FROM nessie.bronze.sales.history

-- Snapshot details
SELECT * FROM nessie.bronze.sales.snapshots
```

### Nessie Branching

```bash
# Create a new branch
curl -X POST http://localhost:19120/api/v2/trees/branch -d '{"name": "dev", "source": "main"}'

# List branches
curl http://localhost:19120/api/v2/trees
```

## Data Flow Details

### Bronze Layer (nessie.bronze.sales)
- **Source**: Raw CSV from S3
- **Schema**: Same as source (PascalCase columns)
- **Added columns**: `ingestion_date`, `ingestion_ts`, `source_file`, `source_system`, `batch_id`
- **Partitioning**: By `ingestion_date`
- **Mode**: Append-only

### Silver Layer (nessie.silver.sales)
- **Source**: Bronze layer
- **Transformations**:
  - Column renaming: `Order ID` → `order_id`, etc.
  - Type casting: `Sales` → `double`, `Quantity` → `int`
  - Text trimming on string columns
  - Null filtering on: `order_id`, `product_id`, `sales`
  - Deduplication on: `order_id` + `product_id`

### Gold Layer (nessie.gold.sales_by_category_region)
- **Source**: Silver layer
- **Aggregation**: By `category` and `region`
- **Metrics**:
  - `total_sales`: Sum of sales (rounded)
  - `total_profit`: Sum of profit (rounded)
  - `total_quantity`: Sum of quantity
  - `order_count`: Count of orders

## Development with Notebooks

The notebooks demonstrate each layer step-by-step:

1. **01_setup_environment.ipynb**: Spark session + Nessie namespaces
2. **02_ingestion_bronze.ipynb**: CSV reading + Bronze table creation
3. **03_silver_transformations.ipynb**: Standardization + quality filters
4. **04_gold_layer.ipynb**: Business aggregations

Run notebooks in VS Code or Jupyter Lab:

```bash
jupyter lab
```

## CI/CD

GitHub Actions validates:
- Project structure
- Python syntax
- Notebook JSON validity
- Requirements file

View status: Actions tab in GitHub

## Troubleshooting

### Nessie Connection Failed
```bash
# Verify Nessie is running
curl http://localhost:19120/api/v2/config

# Restart Nessie Docker container
docker restart <container-id>
```

### S3 Access Denied
- Verify AWS credentials in `.env`
- Check bucket permissions
- Ensure correct region setting

### Out of Memory (Spark)
```bash
# Increase Spark driver memory
export PYSPARK_DRIVER_MEMORY=4g
```

### Hadoop on Windows
Ensure `HADOOP_HOME` is set in `spark_session.py`:
```python
os.environ["HADOOP_HOME"] = r"C:\hadoop"
```

## Key Technical Decisions

1. **Why Iceberg?** ACID transactions, time travel, schema evolution
2. **Why Nessie?** Git-like versioning for data, branch-based development
3. **Why S3?** Scalable object storage, decoupled storage/compute
4. **Why Bronze/Silver/Gold?** Clear separation of concerns, quality boundaries
5. **Why Python + PySpark?** Industry standard, rich ecosystem

## Future Enhancements

- [ ] DBT integration for transformations
- [ ] Airflow/Prefect orchestration
- [ ] Data quality with Great Expectations
- [ ] Unit tests for transformations
- [ ] CI/CD with Nessie branch promotion
- [ ] Monitoring and observability

## License

MIT License - feel free to use this project for learning and demonstration.

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Project Nessie Documentation](https://projectnessie.org/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
