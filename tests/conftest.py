
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("tests") \
        .getOrCreate()

    yield spark  # retorna o spark pro teste e depois para o finaliza

    spark.stop()
