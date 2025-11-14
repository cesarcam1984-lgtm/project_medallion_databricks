from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA silver")

df_mkt_bronze = spark.table("smartdata.bronze.marketing_raw")

df_mkt_silver = (
    df_mkt_bronze
        .withColumn("Income", F.col("Income").cast("double"))
        .withColumn("Dt_Customer", F.col("Dt_Customer").cast("date"))
        .dropDuplicates()
)

df_mkt_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("smartdata.silver.marketing_silver")
