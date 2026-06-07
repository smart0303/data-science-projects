"""Shared Spark session factory with Delta Lake support."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "BatchAnalyticsPipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
