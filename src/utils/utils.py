

from datetime import datetime
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame


def transform_silver(df: DataFrame):
    df_transformed = (
        df.distinct()
        .drop("created_bronze")
        .drop("created_ts_bronze")
        .withColumn("created_silver", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .withColumn("created_ts_silver", lit(datetime.now()))
    )

    return df_transformed


def transform_bronze(df: DataFrame):
    transformed = (
    df.withColumn("created_bronze", lit(datetime.now().strftime("%Y-%m-%d")))
    .withColumn("created_ts_bronze", lit(datetime.now()))
)
    return transformed