# from databricks.sdk.runtime import spark ## só coloque esse import se fizer a conexão de integração com o databricks
from pyspark.sql import DataFrame
from sdp_teste import taxis


def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5
