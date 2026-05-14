from pyspark import pipelines as dp
from pyspark.sql.functions import col
from sdp_teste import utils

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.




@dp.table
def bronze_tb_customer_sdp():
    df = ( spark.read.format("csv")
    .option("header", True)
    .load("/Volumes/ecommerce/raw/system/customer"))

    transformed = utils.transform_bronze(df)

    return transformed