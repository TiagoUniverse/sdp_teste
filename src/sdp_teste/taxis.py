# from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def find_all_taxis(spark) -> DataFrame:
    """Find all taxi data."""
    return spark.read.table("samples.nyctaxi.trips")
