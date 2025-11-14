from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA gold")

df_mkt = spark.table("smartdata.silver.marketing_silver")
df_ecom = spark.table("smartdata.silver.ecommerce_silver")

df_gold = df_ecom.join(
    df_mkt,
    df_ecom["Customer_Segment"] == df_mkt["Marital_Status"],
    "inner"
)

df_gold.write.format("delta").mode("overwrite") \
    .saveAsTable("smartdata.gold.sales_marketing_gold")
