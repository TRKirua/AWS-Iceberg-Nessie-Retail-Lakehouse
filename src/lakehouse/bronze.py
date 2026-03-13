"""
Bronze layer module for raw data ingestion.

This module handles the ingestion of raw CSV data into the Bronze layer.
The Bronze layer keeps data as close to the source as possible, with only
technical metadata added for traceability.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, current_date, lit


def read_sales_csv(
    spark,
    s3_path: str,
    source_file: str = "superstore_sales.csv",
) -> DataFrame:
    """
    Read raw sales CSV from S3 with proper parsing options.

    Args:
        spark: Spark session
        s3_path: S3 path to the CSV file (e.g., s3a://bucket/raw/sales/superstore_sales.csv)
        source_file: Source filename for metadata tracking

    Returns:
        DataFrame with raw sales data
    """
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(s3_path)
    )
    return df


def enrich_with_metadata(
    df: DataFrame,
    source_file: str = "superstore_sales.csv",
    source_system: str = "superstore_csv",
    batch_id: str = "batch_001",
) -> DataFrame:
    """
    Enrich raw data with technical ingestion metadata.

    Args:
        df: Raw input DataFrame
        source_file: Source filename
        source_system: Source system identifier
        batch_id: Batch identifier for multi-batch scenarios

    Returns:
        DataFrame with added metadata columns:
        - ingestion_date: Date of ingestion
        - ingestion_ts: Timestamp of ingestion
        - source_file: Source filename
        - source_system: Source system identifier
        - batch_id: Batch identifier
    """
    return (
        df.withColumn("ingestion_date", current_date())
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", lit(source_file))
        .withColumn("source_system", lit(source_system))
        .withColumn("batch_id", lit(batch_id))
    )


def create_bronze_table(df: DataFrame, table_name: str = "nessie.bronze.sales"):
    """
    Create or replace Bronze table with partitioning.

    Args:
        df: Prepared DataFrame with metadata
        table_name: Full table name including catalog
    """
    from pyspark.sql.functions import col

    (
        df.writeTo(table_name)
        .using("iceberg")
        .partitionedBy(col("ingestion_date"))
        .create()
    )


def append_to_bronze(df: DataFrame, table_name: str = "nessie.bronze.sales"):
    """
    Append data to existing Bronze table.

    Args:
        df: Prepared DataFrame with metadata
        table_name: Full table name including catalog
    """
    df.writeTo(table_name).append()


def drop_bronze_table(spark, table_name: str = "nessie.bronze.sales"):
    """
    Drop Bronze table if exists.

    Args:
        spark: Spark session
        table_name: Full table name including catalog
    """
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def get_bronze_history(spark, table_name: str = "nessie.bronze.sales"):
    """
    Get snapshot history for Bronze table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with snapshot history
    """
    return spark.sql(f"SELECT * FROM {table_name}.history")


def get_bronze_snapshots(spark, table_name: str = "nessie.bronze.sales"):
    """
    Get snapshots for Bronze table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with snapshot details
    """
    return spark.sql(f"SELECT * FROM {table_name}.snapshots")
