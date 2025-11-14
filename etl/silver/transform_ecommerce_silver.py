from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA silver")

df_ecom_bronze = spark.table("smartdata.bronze.ecommerce_raw")

df_ecom_silver = (
    df_ecom_bronze
        .withColumn("Price", F.col("Price").cast("double"))
        .withColumn("Discount", F.col("Discount").cast("double"))
        .withColumn("Marketing_Spend", F.col("Marketing_Spend").cast("double"))
)

df_ecom_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("smartdata.silver.ecommerce_silver")
