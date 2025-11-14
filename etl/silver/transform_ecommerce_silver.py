# etl/silver/transform_ecommerce_silver.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transform_ecommerce_silver(df):
    """
    Aplica las transformaciones necesarias al dataset de ecommerce
    para generar la capa Silver.

    Transformaciones:
    - Casting de Price, Discount y Marketing_Spend a double
    """
    df_silver = (
        df.withColumn("Price", F.col("Price").cast("double"))
          .withColumn("Discount", F.col("Discount").cast("double"))
          .withColumn("Marketing_Spend", F.col("Marketing_Spend").cast("double"))
          .dropDuplicates()
    )
    return df_silver


def main():
    # Crear Spark session
    spark = SparkSession.builder.getOrCreate()

    # Seleccionar catálogo y esquema
    spark.sql("USE CATALOG smartdata")
    spark.sql("USE SCHEMA silver")

    # Leer tabla Bronze
    df_ecom_bronze = spark.table("smartdata.bronze.ecommerce_raw")

    # Aplicar transformaciones Silver
    df_ecom_silver = transform_ecommerce_silver(df_ecom_bronze)

    # Guardar tabla Silver en Delta
    df_ecom_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("smartdata.silver.ecommerce_silver")

    print("✔ Ecommerce Silver generado correctamente.")


if __name__ == "__main__":
    main()
