from pyspark import pipelines as dp
from pyspark.sql.functions import col
from sdp_teste import utils

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.



@dp.table
def silver_tb_customer_sdp():
    df = spark.read.table("bronze_tb_customer_sdp")
    df_transformed = utils.transform_silver(df)

    return df_transformed