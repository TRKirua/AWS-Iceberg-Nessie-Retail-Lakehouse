#!/usr/bin/env python3
"""
Full pipeline orchestration script.

This script runs the complete Bronze -> Silver -> Gold pipeline.
It's designed to be the main entry point for batch processing.

Usage:
    python scripts/run_full_pipeline.py [options]

Examples:
    # Run full pipeline with defaults
    python scripts/run_full_pipeline.py

    # Run with specific source file
    python scripts/run_full_pipeline.py --source-file sales_batch_1.csv

    # Run and drop all tables first
    python scripts/run_full_pipeline.py --drop-all

    # Append to existing Bronze instead of recreating
    python scripts/run_full_pipeline.py --append-bronze
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
    create_bronze_table,
    append_to_bronze,
    drop_bronze_table,
)
from lakehouse.silver import (
    transform_to_silver,
    create_silver_table,
    drop_silver_table,
    run_quality_checks,
)
from lakehouse.gold import (
    create_sales_by_category_region,
    create_gold_table,
    drop_gold_table,
)
from lakehouse.settings import AWS_BUCKET


def setup_namespaces(spark: SparkSession) -> None:
    """Create Nessie namespaces if they don't exist."""
    print("=" * 60)
    print("STEP 0: Setting up Nessie namespaces")
    print("=" * 60)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    print("Namespaces ready.\n")


def run_bronze(
    spark: SparkSession,
    source_file: str = "superstore_sales.csv",
    batch_id: str = "batch_001",
    append: bool = False,
    drop_first: bool = False,
) -> int:
    """
    Run Bronze layer ingestion.

    Returns:
        Number of records in Bronze table after ingestion
    """
    print("=" * 60)
    print("STEP 1: Bronze Layer - Raw Data Ingestion")
    print("=" * 60)

    s3_path = f"s3a://{AWS_BUCKET}/raw/sales/{source_file}"
    print(f"Source: {s3_path}")

    # Read and enrich
    sales_raw = read_sales_csv(spark, s3_path, source_file)
    raw_count = sales_raw.count()
    print(f"Raw records read: {raw_count:,}")

    sales_bronze = enrich_with_metadata(
        sales_raw,
        source_file=source_file,
        source_system="superstore_csv",
        batch_id=batch_id,
    )

    # Drop if requested
    if drop_first:
        print(f"Dropping table nessie.bronze.sales...")
        drop_bronze_table(spark, "nessie.bronze.sales")

    # Write
    if append:
        print("Appending to nessie.bronze.sales...")
        append_to_bronze(sales_bronze, "nessie.bronze.sales")
    else:
        print("Creating nessie.bronze.sales...")
        create_bronze_table(sales_bronze, "nessie.bronze.sales")

    final_count = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
    print(f"Bronze table count: {final_count:,}\n")

    return final_count


def run_silver(spark: SparkSession, drop_first: bool = False) -> int:
    """
    Run Silver layer transformation.

    Returns:
        Number of records in Silver table after transformation
    """
    print("=" * 60)
    print("STEP 2: Silver Layer - Standardization & Quality")
    print("=" * 60)

    # Read and transform
    sales_bronze = spark.table("nessie.bronze.sales")
    bronze_count = sales_bronze.count()
    print(f"Bronze input records: {bronze_count:,}")

    sales_silver = transform_to_silver(sales_bronze)
    silver_count = sales_silver.count()
    print(f"Silver output records: {silver_count:,}")
    print(f"Records filtered: {bronze_count - silver_count:,}")

    # Drop if requested
    if drop_first:
        print(f"Dropping table nessie.silver.sales...")
        drop_silver_table(spark, "nessie.silver.sales")

    # Write
    print("Creating nessie.silver.sales...")
    create_silver_table(sales_silver, "nessie.silver.sales")

    # Quality checks
    print("\nQuality checks:")
    checks = run_quality_checks(spark, "nessie.silver.sales")
    print(f"  Null critical fields: {sum(checks['null_checks'].values())}")
    print(f"  Negative sales: {checks['negative_sales']}")
    print(f"  Invalid quantity: {checks['invalid_quantity']}")
    print(f"  Invalid discount: {checks['invalid_discount']}")
    print()

    return silver_count


def run_gold(spark: SparkSession, drop_first: bool = False) -> int:
    """
    Run Gold layer aggregation.

    Returns:
        Number of records in Gold table after aggregation
    """
    print("=" * 60)
    print("STEP 3: Gold Layer - Business Aggregations")
    print("=" * 60)

    # Read and aggregate
    sales_silver = spark.table("nessie.silver.sales")
    silver_count = sales_silver.count()
    print(f"Silver input records: {silver_count:,}")

    sales_gold = create_sales_by_category_region(sales_silver)
    gold_count = sales_gold.count()
    print(f"Gold aggregated records: {gold_count:,}")

    # Drop if requested
    if drop_first:
        print(f"Dropping table nessie.gold.sales_by_category_region...")
        drop_gold_table(spark, "nessie.gold.sales_by_category_region")

    # Write
    print("Creating nessie.gold.sales_by_category_region...")
    create_gold_table(sales_gold, "nessie.gold.sales_by_category_region")

    # Show top categories
    print("\nTop 5 by sales:")
    spark.sql(
        """
        SELECT category, region, total_sales, total_profit, order_count
        FROM nessie.gold.sales_by_category_region
        ORDER BY total_sales DESC
        LIMIT 5
        """
    ).show(truncate=False)
    print()

    return gold_count


def run_pipeline(
    source_file: str = "superstore_sales.csv",
    batch_id: str = "batch_001",
    drop_all: bool = False,
    append_bronze: bool = False,
) -> dict:
    """
    Run the complete Bronze -> Silver -> Gold pipeline.

    Returns:
        Dictionary with pipeline statistics
    """
    spark = get_spark("full-pipeline")

    try:
        # Setup
        setup_namespaces(spark)

        # Bronze
        bronze_count = run_bronze(
            spark,
            source_file=source_file,
            batch_id=batch_id,
            append=append_bronze,
            drop_first=drop_all,
        )

        # Silver
        silver_count = run_silver(spark, drop_first=drop_all)

        # Gold
        gold_count = run_gold(spark, drop_first=drop_all)

        # Summary
        print("=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Bronze records: {bronze_count:,}")
        print(f"Silver records: {silver_count:,}")
        print(f"Gold records:   {gold_count:,}")
        print(f"Quality filter rate: {(1 - silver_count/bronze_count)*100:.2f}%")
        print("=" * 60)
        print("Pipeline completed successfully!")

        return {
            "bronze": bronze_count,
            "silver": silver_count,
            "gold": gold_count,
        }

    except Exception as e:
        print(f"\nPipeline failed: {e}")
        raise
    finally:
        spark.stop()
        print("\nSpark session stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Run the complete Bronze -> Silver -> Gold pipeline"
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
        "--drop-all",
        action="store_true",
        help="Drop all tables before running pipeline",
    )
    parser.add_argument(
        "--append-bronze",
        action="store_true",
        help="Append to Bronze instead of creating new",
    )

    args = parser.parse_args()

    run_pipeline(
        source_file=args.source_file,
        batch_id=args.batch_id,
        drop_all=args.drop_all,
        append_bronze=args.append_bronze,
    )


if __name__ == "__main__":
    main()
