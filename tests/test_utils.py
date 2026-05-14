from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from utils.utils import transform_bronze, transform_silver
from pyspark.testing import assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, TimestampType



def test_transform_bronze():
    df = spark.createDataFrame(
        [
            (1, "John Doe", 30),
            (2, "Jane Smith", 25),
        ],
        ["id", "name", "age"],
    )

    transformed_df = transform_bronze(df)

    assert "created_bronze" in transformed_df.columns
    assert "created_ts_bronze" in transformed_df.columns


def test_tranform_silver():
    df = spark.createDataFrame(
        [
            (1, "John Doe", 30, "2024-01-01"),
            (2, "Jane Smith", 25, "2024-01-02"),
        ],
        ["id", "name", "age", "created_bronze"],
    )

    transformed_df = transform_silver(df)

    assert "created_silver" in transformed_df.columns
    assert "created_ts_silver" in transformed_df.columns


def test_silver_schema():

    schema_bronze = StructType(
        [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_bronze", StringType(), True),
        StructField("created_ts_bronze", TimestampType(), True)
        ]
    )

    data = [
        ("1", "John Doe", "john.doe@example.com", "1990-01-01", "2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01 00:00:00"),
        ("2", "Jane Smith", "jane.smith@example.com", "1985-05-15", "2024-01-02", "2024-01-02", "2024-01-02", "2024-01-02 00:00:00")
    ]

    df =spark.createDataframe(data, schema_bronze)

    transformed_df = transform_silver(df)

    expected_schema = StructType(
        [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_silver", StringType(), True),
        StructField("created_ts_silver", TimestampType(), True)
        ]
    )

    assertSchemaEqual(transformed_df.schema, expected_schema)