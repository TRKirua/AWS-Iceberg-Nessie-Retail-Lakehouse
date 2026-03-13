#!/usr/bin/env python3
"""
Silver layer transformation script.

This script transforms Bronze data into the Silver layer.
It applies column standardization, data quality filters, and normalization.

Usage:
    python scripts/create_silver_tables.py [--source-table <table>] [--drop-first]

Examples:
    # Basic usage
    python scripts/create_silver_tables.py

    # Specify custom source table
    python scripts/create_silver_tables.py --source-table nessie.bronze.sales

    # Drop and recreate table
    python scripts/create_silver_tables.py --drop-first
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
from lakehouse.silver import (
    transform_to_silver,
    create_silver_table,
    drop_silver_table,
    run_quality_checks,
    get_silver_history,
    get_silver_snapshots,
)


def transform_silver(
    spark: SparkSession,
    source_table: str = "nessie.bronze.sales",
    target_table: str = "nessie.silver.sales",
    drop_first: bool = False,
) -> None:
    """
    Transform Bronze data to Silver layer.

    Args:
        spark: Spark session
        source_table: Source Bronze table name
        target_table: Target Silver table name
        drop_first: If True, drop table before transformation
    """
    # Read Bronze data
    print(f"Reading from {source_table}...")
    sales_bronze = spark.table(source_table)
    bronze_count = sales_bronze.count()
    print(f"Bronze records: {bronze_count:,}")

    # Transform to Silver
    print("Applying Silver transformations...")
    sales_silver = transform_to_silver(sales_bronze)
    silver_count = sales_silver.count()
    print(f"Silver records after transformation: {silver_count:,}")
    print(f"Records filtered: {bronze_count - silver_count:,}")

    # Drop table if requested
    if drop_first:
        print(f"Dropping table {target_table}...")
        drop_silver_table(spark, target_table)

    # Write Silver table
    print(f"Creating table {target_table}...")
    create_silver_table(sales_silver, target_table)

    # Run quality checks
    print("\nRunning quality checks...")
    checks = run_quality_checks(spark, target_table)
    print(f"Total records: {checks['total_records']:,}")
    print(f"Null order_id: {checks['null_checks']['null_order_id']}")
    print(f"Null product_id: {checks['null_checks']['null_product_id']}")
    print(f"Null sales: {checks['null_checks']['null_sales']}")
    print(f"Negative sales: {checks['negative_sales']}")
    print(f"Invalid quantity: {checks['invalid_quantity']}")
    print(f"Invalid discount: {checks['invalid_discount']}")

    # Show history
    print("\nSnapshot history:")
    get_silver_history(spark, target_table).show(truncate=False)

    print("\nSilver transformation completed.")


def main():
    parser = argparse.ArgumentParser(
        description="Transform Bronze data to Silver layer"
    )
    parser.add_argument(
        "--source-table",
        default="nessie.bronze.sales",
        help="Source Bronze table (default: nessie.bronze.sales)",
    )
    parser.add_argument(
        "--target-table",
        default="nessie.silver.sales",
        help="Target Silver table (default: nessie.silver.sales)",
    )
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="Drop table before transformation",
    )

    args = parser.parse_args()

    # Create Spark session
    print("Creating Spark session...")
    spark = get_spark("silver-transformations")

    try:
        # Run transformation
        transform_silver(
            spark,
            source_table=args.source_table,
            target_table=args.target_table,
            drop_first=args.drop_first,
        )

    except Exception as e:
        print(f"Error during Silver transformation: {e}")
        raise
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
