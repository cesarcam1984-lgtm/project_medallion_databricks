# etl/silver/transform_marketing_silver.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def transform_marketing_silver(df):
    """
    Transformaciones Silver:
    - Income → double
    - Dt_Customer → date
    - Quita duplicados
    """
    df_silver = (
        df.withColumn("Income", F.col("Income").cast("double"))
          .withColumn("Dt_Customer", F.to_date("Dt_Customer"))
          .dropDuplicates()
    )
    return df_silver


def main():
    spark = SparkSession.builder.getOrCreate()

    spark.sql("USE CATALOG smartdata")
    spark.sql("USE SCHEMA silver")

    # Cargar Bronze real
    df_mkt_bronze = spark.table("smartdata.bronze.marketing_raw")

    df_mkt_silver = transform_marketing_silver(df_mkt_bronze)

    df_mkt_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("smartdata.silver.marketing_silver")

    print("✔ Marketing Silver generado correctamente.")


if __name__ == "__main__":
    main()
