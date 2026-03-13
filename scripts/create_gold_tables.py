#!/usr/bin/env python3
"""
Gold layer aggregation script.

This script creates business-ready analytical tables from Silver data.
It computes aggregations and creates reporting-ready datasets.

Usage:
    python scripts/create_gold_tables.py [--source-table <table>] [--drop-first]

Examples:
    # Basic usage
    python scripts/create_gold_tables.py

    # Specify custom source table
    python scripts/create_gold_tables.py --source-table nessie.silver.sales

    # Drop and recreate table
    python scripts/create_gold_tables.py --drop-first
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
from lakehouse.gold import (
    create_sales_by_category_region,
    create_gold_table,
    drop_gold_table,
    get_gold_history,
    get_gold_summary,
)


def create_gold(
    spark: SparkSession,
    source_table: str = "nessie.silver.sales",
    target_table: str = "nessie.gold.sales_by_category_region",
    drop_first: bool = False,
) -> None:
    """
    Create Gold layer aggregations from Silver data.

    Args:
        spark: Spark session
        source_table: Source Silver table name
        target_table: Target Gold table name
        drop_first: If True, drop table before creation
    """
    # Read Silver data
    print(f"Reading from {source_table}...")
    sales_silver = spark.table(source_table)
    silver_count = sales_silver.count()
    print(f"Silver records: {silver_count:,}")

    # Create Gold aggregation
    print("Creating Gold aggregation: sales_by_category_region...")
    sales_gold = create_sales_by_category_region(sales_silver)
    gold_count = sales_gold.count()
    print(f"Gold aggregated records: {gold_count:,}")

    # Show sample
    print("\nSample Gold data:")
    sales_gold.show(20, truncate=False)

    # Drop table if requested
    if drop_first:
        print(f"Dropping table {target_table}...")
        drop_gold_table(spark, target_table)

    # Write Gold table
    print(f"Creating table {target_table}...")
    create_gold_table(sales_gold, target_table)

    # Show summary
    print("\nGold summary (by total_sales):")
    get_gold_summary(spark, target_table).show(truncate=False)

    # Show history
    print("\nSnapshot history:")
    get_gold_history(spark, target_table).show(truncate=False)

    print("\nGold layer creation completed.")


def main():
    parser = argparse.ArgumentParser(
        description="Create Gold layer aggregations from Silver data"
    )
    parser.add_argument(
        "--source-table",
        default="nessie.silver.sales",
        help="Source Silver table (default: nessie.silver.sales)",
    )
    parser.add_argument(
        "--target-table",
        default="nessie.gold.sales_by_category_region",
        help="Target Gold table (default: nessie.gold.sales_by_category_region)",
    )
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="Drop table before creation",
    )

    args = parser.parse_args()

    # Create Spark session
    print("Creating Spark session...")
    spark = get_spark("gold-aggregations")

    try:
        # Run Gold creation
        create_gold(
            spark,
            source_table=args.source_table,
            target_table=args.target_table,
            drop_first=args.drop_first,
        )

    except Exception as e:
        print(f"Error during Gold creation: {e}")
        raise
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
