"""
Silver layer module for data standardization and quality.

This module handles the transformation of Bronze data into the Silver layer.
The Silver layer applies:
- Column standardization (renaming to snake_case)
- Data type validation and casting
- Data quality filters (null handling, deduplication)
- Light normalization (trim, etc.)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim


def standardize_sales_columns(silver_bronze: DataFrame) -> DataFrame:
    """
    Standardize column names from source format to snake_case.

    Maps source columns (e.g., "Order ID") to target columns (e.g., "order_id").
    Preserves all technical metadata columns from Bronze.

    Args:
        silver_bronze: Bronze DataFrame

    Returns:
        DataFrame with standardized column names and types
    """
    return (
        silver_bronze.select(
            col("Order ID").alias("order_id"),
            col("Order Date").alias("order_date"),
            col("Ship Date").alias("ship_date"),
            col("Ship Mode").alias("ship_mode"),
            col("Customer ID").alias("customer_id"),
            col("Customer Name").alias("customer_name"),
            col("Segment").alias("segment"),
            col("Country").alias("country"),
            col("City").alias("city"),
            col("State").alias("state"),
            col("Postal Code").alias("postal_code"),
            col("Region").alias("region"),
            col("Product ID").alias("product_id"),
            col("Category").alias("category"),
            col("Sub-Category").alias("sub_category"),
            col("Product Name").alias("product_name"),
            col("Sales").cast("double").alias("sales"),
            col("Quantity").cast("int").alias("quantity"),
            col("Discount").cast("double").alias("discount"),
            col("Profit").cast("double").alias("profit"),
            col("ingestion_date"),
            col("ingestion_ts"),
            col("source_file"),
            col("source_system"),
            col("batch_id"),
        )
        .withColumn("ship_mode", trim(col("ship_mode")))
        .withColumn("segment", trim(col("segment")))
        .withColumn("country", trim(col("country")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("region", trim(col("region")))
        .withColumn("category", trim(col("category")))
        .withColumn("sub_category", trim(col("sub_category")))
        .withColumn("product_name", trim(col("product_name")))
    )


def apply_quality_filters(df: DataFrame) -> DataFrame:
    """
    Apply data quality filters to Silver DataFrame.

    Quality rules:
    - Remove rows with null critical fields (order_id, product_id, sales)
    - Remove duplicates based on natural key (order_id, product_id)

    Args:
        df: Standardized DataFrame

    Returns:
        Filtered DataFrame
    """
    return df.dropna(subset=["order_id", "product_id", "sales"]).dropDuplicates(
        ["order_id", "product_id"]
    )


def transform_to_silver(silver_bronze: DataFrame) -> DataFrame:
    """
    Complete Bronze to Silver transformation pipeline.

    This function:
    1. Standardizes column names and types
    2. Applies text normalization
    3. Filters null values on critical fields
    4. Removes duplicates

    Args:
        silver_bronze: Bronze DataFrame

    Returns:
        Transformed Silver DataFrame
    """
    return apply_quality_filters(standardize_sales_columns(silver_bronze))


def create_silver_table(df: DataFrame, table_name: str = "nessie.silver.sales"):
    """
    Create or replace Silver table.

    Args:
        df: Transformed Silver DataFrame
        table_name: Full table name including catalog
    """
    df.writeTo(table_name).using("iceberg").createOrReplace()


def drop_silver_table(spark, table_name: str = "nessie.silver.sales"):
    """
    Drop Silver table if exists.

    Args:
        spark: Spark session
        table_name: Full table name including catalog
    """
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def get_silver_history(spark, table_name: str = "nessie.silver.sales"):
    """
    Get snapshot history for Silver table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with snapshot history
    """
    return spark.sql(f"SELECT * FROM {table_name}.history")


def get_silver_snapshots(spark, table_name: str = "nessie.silver.sales"):
    """
    Get snapshots for Silver table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with snapshot details
    """
    return spark.sql(f"SELECT * FROM {table_name}.snapshots")


def run_quality_checks(spark, silver_table: str = "nessie.silver.sales"):
    """
    Run data quality checks on Silver table.

    Checks:
    - Null values on critical fields
    - Negative sales values
    - Invalid quantities
    - Invalid discounts

    Args:
        spark: Spark session
        silver_table: Full Silver table name

    Returns:
        Dictionary with check results
    """
    from pyspark.sql.functions import sum as spark_sum, when

    df = spark.table(silver_table)

    null_checks = (
        df.select(
            spark_sum(when(col("order_id").isNull(), 1).otherwise(0)).alias(
                "null_order_id"
            ),
            spark_sum(when(col("product_id").isNull(), 1).otherwise(0)).alias(
                "null_product_id"
            ),
            spark_sum(when(col("sales").isNull(), 1).otherwise(0)).alias("null_sales"),
        )
        .first()
        .asDict()
    )

    negative_sales = df.filter(col("sales") < 0).count()
    invalid_quantity = df.filter(col("quantity") <= 0).count()
    invalid_discount = df.filter((col("discount") < 0) | (col("discount") > 1)).count()

    return {
        "null_checks": null_checks,
        "negative_sales": negative_sales,
        "invalid_quantity": invalid_quantity,
        "invalid_discount": invalid_discount,
        "total_records": df.count(),
    }
