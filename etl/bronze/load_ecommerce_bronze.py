from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA bronze")

df_ecom_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .load("dbfs:/FileStore/ecommerce_data.csv")
)

df_ecom_bronze.write.format("delta").mode("overwrite") \
    .saveAsTable("smartdata.bronze.ecommerce_raw")
