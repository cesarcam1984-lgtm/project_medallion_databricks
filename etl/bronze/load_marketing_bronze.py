from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA bronze")

df_mkt_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .load("dbfs:/FileStore/marketing_campaign.csv")
)

df_mkt_bronze.write.format("delta").mode("overwrite") \
    .saveAsTable("smartdata.bronze.marketing_raw")
