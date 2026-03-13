#!/usr/bin/env python3
"""
Bronze layer ingestion script.

This script ingests raw CSV data into the Bronze layer.
It reads from S3, adds technical metadata, and writes to an Iceberg table.

Usage:
    python scripts/create_bronze_tables.py [--source-file <filename>] [--batch-id <batch_id>]

Examples:
    # Basic usage with default values
    python scripts/create_bronze_tables.py

    # Specify custom source file and batch ID
    python scripts/create_bronze_tables.py --source-file sales_batch_1.csv --batch-id batch_001

    # Append mode (add to existing table)
    python scripts/create_bronze_tables.py --append
"""

import argparse
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).resolve().parents[1]
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession

from lakehouse.spark_session import get_spark
from lakehouse.settings import AWS_BUCKET
from lakehouse.bronze import (
    read_sales_csv,
    enrich_with_metadata,
    create_bronze_table,
    append_to_bronze,
    drop_bronze_table,
    get_bronze_history,
    get_bronze_snapshots,
)


def setup_namespaces(spark: SparkSession) -> None:
    """
    Create Nessie namespaces if they don't exist.

    Args:
        spark: Spark session
    """
    print("Setting up Nessie namespaces...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    print("Namespaces ready.")


def ingest_bronze(
    spark: SparkSession,
    source_file: str = "superstore_sales.csv",
    batch_id: str = "batch_001",
    append: bool = False,
    table_name: str = "nessie.bronze.sales",
    drop_first: bool = False,
) -> None:
    """
    Ingest raw CSV data into Bronze layer.

    Args:
        spark: Spark session
        source_file: Source CSV filename
        batch_id: Batch identifier
        append: If True, append to existing table; if False, create new
        table_name: Target table name
        drop_first: If True, drop table before ingestion
    """
    # Build S3 path
    s3_path = f"s3a://{AWS_BUCKET}/raw/sales/{source_file}"
    print(f"Reading from: {s3_path}")

    # Read raw data
    sales_raw = read_sales_csv(spark, s3_path, source_file)
    raw_count = sales_raw.count()
    print(f"Raw records read: {raw_count:,}")

    # Enrich with metadata
    sales_bronze = enrich_with_metadata(
        sales_raw,
        source_file=source_file,
        source_system="superstore_csv",
        batch_id=batch_id,
    )
    print(f"Metadata columns added.")

    # Drop table if requested
    if drop_first:
        print(f"Dropping table {table_name}...")
        drop_bronze_table(spark, table_name)

    # Write to Bronze
    if append:
        print(f"Appending to {table_name}...")
        append_to_bronze(sales_bronze, table_name)
    else:
        print(f"Creating table {table_name}...")
        create_bronze_table(sales_bronze, table_name)

    # Verify
    final_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").first()[0]
    print(f"Final table count: {final_count:,}")

    # Show history
    print("\nSnapshot history:")
    get_bronze_history(spark, table_name).show(truncate=False)

    print("\nBronze ingestion completed.")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest raw CSV data into Bronze layer"
    )
    parser.add_argument(
        "--source-file",
        default="superstore_sales.csv",
        help="Source CSV filename (default: superstore_sales.csv)",
    )
    parser.add_argument(
        "--batch-id",
        default="batch_001",
        help="Batch identifier (default: batch_001)",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing table instead of creating new",
    )
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="Drop table before ingestion",
    )
    parser.add_argument(
        "--table-name",
        default="nessie.bronze.sales",
        help="Target table name (default: nessie.bronze.sales)",
    )

    args = parser.parse_args()

    # Create Spark session
    print("Creating Spark session...")
    spark = get_spark("bronze-ingestion")

    try:
        # Setup namespaces
        setup_namespaces(spark)

        # Run ingestion
        ingest_bronze(
            spark,
            source_file=args.source_file,
            batch_id=args.batch_id,
            append=args.append,
            table_name=args.table_name,
            drop_first=args.drop_first,
        )

    except Exception as e:
        print(f"Error during Bronze ingestion: {e}")
        raise
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
