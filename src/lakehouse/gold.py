"""
Gold layer module for business-ready analytical tables.

This module handles the creation of aggregated analytical tables
from the Silver layer. The Gold layer contains:
- Business-oriented aggregations
- Reporting-ready metrics
- Rounded values for presentation
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum, count, round as spark_round


def create_sales_by_category_region(
    silver_df: DataFrame,
) -> DataFrame:
    """
    Create Gold aggregation: sales by category and region.

    Aggregates:
    - total_sales: Sum of sales (rounded to 2 decimals)
    - total_profit: Sum of profit (rounded to 2 decimals)
    - total_quantity: Sum of quantity
    - order_count: Count of distinct orders

    Args:
        silver_df: Silver DataFrame

    Returns:
        Aggregated Gold DataFrame
    """
    return (
        silver_df.groupBy("category", "region")
        .agg(
            spark_round(spark_sum("sales"), 2).alias("total_sales"),
            spark_round(spark_sum("profit"), 2).alias("total_profit"),
            spark_sum("quantity").alias("total_quantity"),
            count("order_id").alias("order_count"),
        )
        .orderBy("category", "region")
    )


def create_gold_table(
    df: DataFrame, table_name: str = "nessie.gold.sales_by_category_region"
):
    """
    Create or replace Gold table.

    Args:
        df: Aggregated Gold DataFrame
        table_name: Full table name including catalog
    """
    df.writeTo(table_name).using("iceberg").createOrReplace()


def drop_gold_table(spark, table_name: str = "nessie.gold.sales_by_category_region"):
    """
    Drop Gold table if exists.

    Args:
        spark: Spark session
        table_name: Full table name including catalog
    """
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def get_gold_history(spark, table_name: str = "nessie.gold.sales_by_category_region"):
    """
    Get snapshot history for Gold table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with snapshot history
    """
    return spark.sql(f"SELECT * FROM {table_name}.history")


def get_gold_snapshots(spark, table_name: str = "nessie.gold.sales_by_category_region"):
    """
    Get snapshots for Gold table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with snapshot details
    """
    return spark.sql(f"SELECT * FROM {table_name}.snapshots")


def get_gold_summary(spark, table_name: str = "nessie.gold.sales_by_category_region"):
    """
    Get summary statistics from Gold table.

    Args:
        spark: Spark session
        table_name: Full table name including catalog

    Returns:
        DataFrame with top metrics by total_sales
    """
    return spark.sql(
        f"""
        SELECT *
        FROM {table_name}
        ORDER BY total_sales DESC
        """
    )
