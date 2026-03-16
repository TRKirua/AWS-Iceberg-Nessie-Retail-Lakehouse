#!/usr/bin/env python3
"""
Full pipeline orchestration script.

This script runs the complete Bronze -> Silver -> Gold pipeline.
It processes batch files from S3 and creates the full medallion architecture.

Usage:
    # Run full pipeline with all 3 batches
    python scripts/run_full_pipeline.py

    # Run with specific batches
    python scripts/run_full_pipeline.py --batches 1 2

    # Run single batch
    python scripts/run_full_pipeline.py --batch 3

    # Run and drop all tables first
    python scripts/run_full_pipeline.py --drop-all
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
    batch_number: int = 1,
    drop_first: bool = False,
) -> int:
    """
    Run Bronze layer ingestion for a specific batch.

    Args:
        spark: Spark session
        batch_number: Batch number (1, 2, or 3)
        drop_first: Drop table before ingestion

    Returns:
        Number of records in Bronze table after ingestion
    """
    print("=" * 60)
    print(f"STEP 1: Bronze Layer - Batch {batch_number} Ingestion")
    print("=" * 60)

    # Construct batch file path
    batch_file = f"sales_batch_{batch_number:03d}.csv"
    s3_path = f"s3a://{AWS_BUCKET}/raw/batches/{batch_file}"
    print(f"Source: {s3_path}")

    # Read and enrich
    sales_raw = read_sales_csv(spark, s3_path, batch_file)
    raw_count = sales_raw.count()
    print(f"Raw records read: {raw_count:,}")

    sales_bronze = enrich_with_metadata(
        sales_raw,
        source_file=batch_file,
        source_system="sales_system",
        batch_id=f"batch_{batch_number}",
    )

    # Drop if requested
    if drop_first:
        print(f"Dropping table nessie.bronze.sales...")
        drop_bronze_table(spark, "nessie.bronze.sales")

    # Write
    if batch_number > 1:
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
    batch_numbers: list[int] = [1, 2, 3],
    drop_all: bool = False,
) -> dict:
    """
    Run the complete Bronze -> Silver -> Gold pipeline for multiple batches.

    Args:
        batch_numbers: List of batch numbers to process (default: [1, 2, 3])
        drop_all: Drop all tables before running pipeline

    Returns:
        Dictionary with pipeline statistics
    """
    spark = get_spark("full-pipeline")

    try:
        # Setup
        setup_namespaces(spark)

        # Process each batch
        for i, batch_num in enumerate(batch_numbers):
            print(f"\n{'='*60}")
            print(f"PROCESSING BATCH {batch_num} ({i+1}/{len(batch_numbers)})")
            print(f"{'='*60}\n")

            run_bronze(
                spark,
                batch_number=batch_num,
                drop_first=(batch_num == 1 and drop_all),
            )

        # Silver (process all Bronze data)
        silver_count = run_silver(spark, drop_first=drop_all)

        # Gold (recalculate from all Silver data)
        gold_count = run_gold(spark, drop_first=drop_all)

        # Summary
        final_bronze_count = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
        print("=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Batches processed: {batch_numbers}")
        print(f"Bronze records: {final_bronze_count:,}")
        print(f"Silver records: {silver_count:,}")
        print(f"Gold records:   {gold_count:,}")
        if final_bronze_count > 0:
            print(f"Quality filter rate: {(1 - silver_count/final_bronze_count)*100:.2f}%")
        print("=" * 60)
        print("Pipeline completed successfully!")

        return {
            "batches": batch_numbers,
            "bronze": final_bronze_count,
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
        "--batches",
        type=int,
        nargs="+",
        default=[1, 2, 3],
        help="Batch numbers to process (default: 1 2 3)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=None,
        help="Single batch number to process (e.g., --batch 1)",
    )
    parser.add_argument(
        "--drop-all",
        action="store_true",
        help="Drop all tables before running pipeline",
    )

    args = parser.parse_args()

    # Determine which batches to process
    if args.batch is not None:
        batch_numbers = [args.batch]
    else:
        batch_numbers = args.batches

    run_pipeline(
        batch_numbers=batch_numbers,
        drop_all=args.drop_all,
    )


if __name__ == "__main__":
    main()
