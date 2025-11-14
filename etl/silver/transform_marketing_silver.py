# etl/silver/transform_marketing_silver.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def transform_marketing_silver(df):
    """
    Aplica las transformaciones necesarias al dataset de marketing
    para generar la capa Silver.

    Transformaciones:
    - Casting de Income a double
    - Casting de Dt_Customer a date
    - Eliminación de duplicados
    """
    df_silver = (
        df.withColumn("Income", F.col("Income").cast("double"))
          .withColumn("Dt_Customer", F.col("Dt_Customer").cast("date"))
          .dropDuplicates()
    )
    return df_silver


def main():
    # Crear SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Seleccionar catálogo y esquema
    spark.sql("USE CATALOG smartdata")
    spark.sql("USE SCHEMA silver")

    # Cargar tabla Bronze
    df_mkt_bronze = spark.table("smartdata.bronze.marketing_raw")

    # Aplicar transformaciones Silver
    df_mkt_silver = transform_marketing_silver(df_mkt_bronze)

    # Guardar tabla Silver en Delta
    df_mkt_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("smartdata.silver.marketing_silver")

    print("✔ Marketing Silver generado correctamente.")


if __name__ == "__main__":
    main()
