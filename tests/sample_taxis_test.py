# from databricks.sdk.runtime import spark ## só coloque esse import se fizer a conexão de integração com o databricks
from pyspark.sql import DataFrame
from sdp_teste import taxis


def test_find_all_taxis(spark):
    results = taxis.find_all_taxis(spark)
    assert results.count() > 5
