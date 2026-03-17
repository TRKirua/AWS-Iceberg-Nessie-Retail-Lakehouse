# Retail Lakehouse on AWS with Apache Iceberg & Nessie

A professional data lakehouse demonstration using:
- **AWS S3** for object storage
- **Apache Iceberg** for ACID table format with time travel
- **Project Nessie** for Git-like data catalog versioning
- **Apache Spark** for data processing
- **Bronze / Silver / Gold** medallion architecture

## Architecture Overview

```
┌────────────────────────────────────────────────────┐
│                      DATA LAKEHOUSE                │
├────────────────────────────────────────────────────┤
│                                                    │
│   S3 Files ──► Bronze ──► Silver ──► Gold          │
│               (Iceberg)  (Iceberg) (Iceberg)       │
│                   │          │         │           │
│                   ▼          ▼         ▼           │
│                 Nessie Catalog (Versioned)         │
│                                                    │
└────────────────────────────────────────────────────┘
```

## Project Structure

```
aws-iceberg-nessie-retail-lakehouse/
├── data/
│   ├── raw/                    # Single source file for initial load
│   │   └── superstore_sales.csv
│   └── raw_batches/            # Multi-batch files for demo
│       ├── sales_batch_1.csv
│       ├── sales_batch_2.csv
│       └── sales_batch_3.csv
├── notebooks/                  # Jupyter notebooks for learning and demo
│   ├── 01_setup_environment.ipynb
│   ├── 02_ingestion_bronze.ipynb
│   ├── 03_silver_transformations.ipynb
│   ├── 04_gold_layer.ipynb
│   ├── 05_nessie_demo.ipynb
│   └── 06_complete_demo.ipynb  # Multi-batch demo for interviews
├── scripts/                    # Executable Python scripts
│   ├── run_full_pipeline.py    # Main pipeline script
│   ├── upload_to_s3.py         # Upload data to S3
│   └── generate_batch_data.py  # Generate test batch files
├── sql/                        # Query templates for each layer
│   ├── bronze_queries.sql
│   ├── silver_queries.sql
│   ├── gold_queries.sql
│   └── iceberg_metadata_queries.sql
└── src/lakehouse/              # Python modules
    ├── spark_session.py        # Spark session builder
    ├── settings.py             # Configuration
    ├── bronze.py               # Bronze layer logic
    ├── silver.py               # Silver layer logic
    └── gold.py                 # Gold layer logic
```

## Prerequisites

### Software Required
- Python 3.11+
- Java 11+ (for Spark)
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
docker run -d --name nessie -p 19120:19120 ghcr.io/projectnessie/nessie:latest
```

Verify Nessie is running:
```bash
curl http://localhost:19120/api/v2/config
```

### 2. Upload Data to S3

```bash
# Upload the main file
python scripts/upload_to_s3.py

# Upload batch files for demo
python scripts/upload_to_s3.py --batch-files
```

### 3. Run the Complete Pipeline

```bash
# Single file ingestion
python scripts/run_full_pipeline.py

# Or with options
python scripts/run_full_pipeline.py --drop-all
```

## Notebooks

### Learning Path (01-04)

| Notebook                          | Content                             |
|-----------------------------------|-------------------------------------|
| `01_setup_environment.ipynb`      | Spark session + Nessie namespaces   |
| `02_ingestion_bronze.ipynb`       | CSV reading + Bronze table creation |
| `03_silver_transformations.ipynb` | Standardization + quality filters   |
| `04_gold_layer.ipynb`             | Business aggregations               |

### Demo Notebooks (05-06)

| Notebook                 | Content                                           |
|--------------------------|---------------------------------------------------|
| `05_nessie_demo.ipynb`   | Nessie features: time travel, branching, rollback |
| `06_complete_demo.ipynb` | **Multi-batch demo for interviews**               |

Run notebooks in VS Code or Jupyter Lab:
```bash
jupyter lab
```

## Layer Responsibilities

| Layer      | Purpose             | Characteristics                                                       |
|------------|---------------------|-----------------------------------------------------------------------|
| **Bronze** | Raw data landing    | Append-only, partitioned by `ingestion_date`, preserves source format |
| **Silver** | Standardization     | Column renaming, quality filters, deduplication, type casting         |
| **Gold**   | Business aggregates | Pre-computed metrics, rounded values, reporting-ready                 |

## CI/CD

GitHub Actions:
- Validates project structure
- Validates Python syntax
- Validates notebook JSON
- **Uploads data to S3 on every push to main branch**

### Required GitHub Secrets

| Secret                  | Description                    |
|-------------------------|--------------------------------|
| `AWS_ACCESS_KEY_ID`     | AWS access key                 |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key                 |
| `AWS_REGION`            | AWS region (e.g., `eu-west-3`) |
| `AWS_BUCKET`            | S3 bucket name                 |

## Interview Demo Guide

### Demo Setup (5 minutes)

```bash
# 1. Start Nessie
docker run -p 19120:19120 ghcr.io/projectnessie/nessie:latest

# 2. Upload batch data to S3
python scripts/upload_to_s3.py --batch-files
```

### Demo Flow (15-20 minutes)

#### Option 1: Use the Complete Demo Notebook (Recommended)

Open `notebooks/06_complete_demo.ipynb` and run all cells. This demonstrates:

1. **Batch 1 (Morning)**: Initial ingestion, Bronze → Silver → Gold
2. **Batch 2 (Noon)**: Append new data, update all layers
3. **Batch 3 (Afternoon)**: Final append, complete dataset
4. **Time Travel**: Query data at different points in time
5. **Branching**: Create a dev branch for safe experimentation
6. **Rollback**: Demonstrate reverting to previous state

#### Option 2: Manual Step-by-Step

```bash
# Step 1: Run initial pipeline
python scripts/run_full_pipeline.py --drop-all

# Step 2: Run Nessie demo notebook
jupyter lab notebooks/05_nessie_demo.ipynb
```

### Key Talking Points

| Feature                    | Benefit                         | Demo Command                               |
|----------------------------|---------------------------------|--------------------------------------------|
| **Medallion Architecture** | Clear separation of concerns    | `show tables in nessie.bronze/silver/gold` |
| **Data Quality**           | Automatic filtering of bad data | Quality check output in pipeline           |
| **Time Travel**            | Query any historical version    | `VERSION AS OF 'snapshot_id'`              |
| **Branching**              | Safe experimentation            | Nessie API calls                           |
| **ACID Transactions**      | Reliable data updates           | Show snapshot history                      |
| **S3 Storage**             | Decoupled compute and storage   | `s3a://` paths                             |

## Nessie & Iceberg Features

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
curl -X POST http://localhost:19120/api/v2/trees -d '{"name": "dev", "source": "main"}'

# List branches
curl http://localhost:19120/api/v2/trees
```

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

## License

MIT License - feel free to use this project for learning and demonstration.
