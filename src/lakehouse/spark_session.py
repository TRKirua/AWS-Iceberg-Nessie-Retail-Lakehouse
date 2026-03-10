import os
from pyspark.sql import SparkSession

from lakehouse.settings import (
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    NESSIE_URI,
    NESSIE_BRANCH,
    WAREHOUSE,
)

def get_spark(app_name: str = "aws-iceberg-nessie-retail-lakehouse") -> SparkSession:
    """
    Create and return a Spark session configured for:
    - AWS S3
    - Apache Iceberg
    - Project Nessie
    """

    # Required on Windows for Spark/Hadoop
    os.environ["HADOOP_HOME"] = r"C:\hadoop"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.103.3",
                "software.amazon.awssdk:bundle:2.25.53",
                "software.amazon.awssdk:url-connection-client:2.25.53",
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ])
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", NESSIE_BRANCH)
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )

    return spark