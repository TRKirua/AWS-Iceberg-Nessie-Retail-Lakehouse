#!/usr/bin/env python3
"""
Multi-batch ingestion script.

This script ingests multiple batch CSV files from a directory into the Bronze layer.
It simulates an event-driven batch processing scenario where multiple files arrive
and need to be processed sequentially.

Usage:
    python scripts/ingest_multi_batch.py [--batch-dir <dir>] [--pattern <pattern>]

Examples:
    # Ingest all batch files from default directory
    python scripts/ingest_multi_batch.py

    # Ingest from custom directory
    python scripts/ingest_multi_batch.py --batch-dir data/raw_batches

    # Process specific files matching pattern
    python scripts/ingest_multi_batch.py --pattern "sales_batch_*.csv"
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
from lakehouse.bronze import (
    read_sales_csv,
    enrich_with_metadata,
    append_to_bronze,
    drop_bronze_table,
    get_bronze_history,
)
from lakehouse.settings import AWS_BUCKET


def setup_namespaces(spark: SparkSession) -> None:
    """Create Nessie namespaces if they don't exist."""
    print("Setting up Nessie namespaces...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    print("Namespaces ready.\n")


def ingest_from_local(
    spark: SparkSession,
    local_path: Path,
    source_file: str,
    batch_id: str,
    table_name: str = "nessie.bronze.sales",
) -> int:
    """
    Ingest a single local CSV file into Bronze.

    Args:
        spark: Spark session
        local_path: Local path to CSV file
        source_file: Source filename for metadata
        batch_id: Batch identifier
        table_name: Target table name

    Returns:
        Number of records ingested
    """
    print(f"\nProcessing: {local_path.name}")

    # Read local file
    sales_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(str(local_path))
    )

    raw_count = sales_raw.count()
    print(f"  Records read: {raw_count:,}")

    # Enrich with metadata
    sales_bronze = enrich_with_metadata(
        sales_raw,
        source_file=source_file,
        source_system="superstore_csv",
        batch_id=batch_id,
    )

    # Append to Bronze
    append_to_bronze(sales_bronze, table_name)
    print(f"  Appended to {table_name}")

    return raw_count


def ingest_multi_batch_from_local(
    spark: SparkSession,
    batch_dir: Path,
    pattern: str = "sales_batch_*.csv",
    table_name: str = "nessie.bronze.sales",
    drop_first: bool = False,
) -> dict:
    """
    Ingest multiple batch files from local directory.

    Args:
        spark: Spark session
        batch_dir: Directory containing batch files
        pattern: Glob pattern to match batch files
        table_name: Target table name
        drop_first: If True, drop table before ingestion

    Returns:
        Dictionary with ingestion statistics
    """
    # Drop table if requested
    if drop_first:
        print(f"Dropping table {table_name}...")
        drop_bronze_table(spark, table_name)
        print()

    # Find batch files
    batch_files = sorted(batch_dir.glob(pattern))

    if not batch_files:
        print(f"No files found matching pattern '{pattern}' in {batch_dir}")
        return {"status": "no_files", "files_processed": 0}

    print(f"Found {len(batch_files)} batch files to process:")
    for f in batch_files:
        print(f"  - {f.name}")
    print()

    # Process each batch
    stats = {
        "status": "success",
        "files_processed": 0,
        "total_records": 0,
        "batches": [],
    }

    for i, batch_file in enumerate(batch_files, 1):
        batch_id = f"batch_{i:03d}"
        record_count = ingest_from_local(
            spark,
            batch_file,
            source_file=batch_file.name,
            batch_id=batch_id,
            table_name=table_name,
        )

        stats["batches"].append({
            "file": batch_file.name,
            "batch_id": batch_id,
            "records": record_count,
        })
        stats["total_records"] += record_count
        stats["files_processed"] = i

    # Show final table count
    final_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").first()[0]
    stats["final_table_count"] = final_count

    print(f"\n{'=' * 60}")
    print(f"Multi-batch ingestion completed!")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Total records ingested: {stats['total_records']:,}")
    print(f"Final table count: {final_count:,}")
    print(f"{'=' * 60}")

    return stats


def ingest_from_s3(
    spark: SparkSession,
    batch_dir: str,
    table_name: str = "nessie.bronze.sales",
    drop_first: bool = False,
) -> dict:
    """
    Ingest batch files directly from S3.

    This method reads all files matching a pattern from S3 in a single
    operation, which is more efficient for large-scale batch processing.

    Args:
        spark: Spark session
        batch_dir: S3 directory path (e.g., s3a://bucket/raw/batches/)
        table_name: Target table name
        drop_first: If True, drop table before ingestion

    Returns:
        Dictionary with ingestion statistics
    """
    # Drop table if requested
    if drop_first:
        print(f"Dropping table {table_name}...")
        drop_bronze_table(spark, table_name)
        print()

    s3_pattern = f"{batch_dir}*.csv"
    print(f"Reading from S3: {s3_pattern}")

    # Read all CSV files from S3
    sales_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(s3_pattern)
    )

    raw_count = sales_raw.count()
    print(f"Records read: {raw_count:,}")

    # For S3 wildcard reads, we can't easily track individual files
    # Use a generic batch_id
    sales_bronze = enrich_with_metadata(
        sales_raw,
        source_file="multi_batch_s3",
        source_system="superstore_csv",
        batch_id="batch_s3_wildcard",
    )

    # Check if table exists to decide append vs create
    table_exists = bool(
        spark.sql(
            f"SHOW TABLES IN nessie LIKE 'sales'"
        ).count()
    )

    if table_exists and not drop_first:
        print(f"Appending to {table_name}...")
        append_to_bronze(sales_bronze, table_name)
    else:
        print(f"Creating {table_name}...")
        from lakehouse.bronze import create_bronze_table
        create_bronze_table(sales_bronze, table_name)

    # Show final count
    final_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").first()[0]

    return {
        "status": "success",
        "total_records": raw_count,
        "final_table_count": final_count,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Ingest multiple batch files into Bronze layer"
    )
    parser.add_argument(
        "--batch-dir",
        type=str,
        default="data/raw_batches",
        help="Directory containing batch files (default: data/raw_batches)",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="sales_batch_*.csv",
        help="Glob pattern to match batch files (default: sales_batch_*.csv)",
    )
    parser.add_argument(
        "--s3-mode",
        action="store_true",
        help="Read files from S3 instead of local filesystem",
    )
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="Drop Bronze table before ingestion",
    )

    args = parser.parse_args()

    # Create Spark session
    print("Creating Spark session...")
    spark = get_spark("multi-batch-ingestion")

    try:
        # Setup namespaces
        setup_namespaces(spark)

        # Run ingestion
        if args.s3_mode:
            # S3 mode - read directly from S3
            s3_path = f"s3a://{AWS_BUCKET}/raw/batches/"
            stats = ingest_from_s3(
                spark,
                s3_path,
                drop_first=args.drop_first,
            )
        else:
            # Local mode - read from local directory
            batch_dir = Path(args.batch_dir)
            if not batch_dir.exists():
                print(f"Error: Batch directory not found: {batch_dir}")
                return 1

            stats = ingest_multi_batch_from_local(
                spark,
                batch_dir,
                pattern=args.pattern,
                drop_first=args.drop_first,
            )

        # Show snapshot history
        print("\nSnapshot history:")
        get_bronze_history(spark, "nessie.bronze.sales").show(truncate=False)

        return 0

    except Exception as e:
        print(f"Error during multi-batch ingestion: {e}")
        raise
    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    exit(main())
