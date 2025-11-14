# =========================================================
#  SILVER - Transformaciones del dataset Ecommerce
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transform_ecommerce_silver(df):
    """
    Aplica las transformaciones necesarias para la capa Silver.

    Transformaciones aplicadas:
    - Conversión de Price, Discount y Marketing_Spend a tipo double
    - Eliminación de duplicados
    """

    df_silver = (
        df.withColumn("Price", F.col("Price").cast("double"))
          .withColumn("Discount", F.col("Discount").cast("double"))
          .withColumn("Marketing_Spend", F.col("Marketing_Spend").cast("double"))
          .dropDuplicates()
    )

    return df_silver


def main():
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("ecommerce_silver").getOrCreate()

    # Seleccionar catálogo y esquema Silver
    spark.sql("USE CATALOG smartdata")
    spark.sql("USE SCHEMA silver")

    print("📥 Cargando datos desde Bronze: ecommerce_raw")

    # Cargar tabla Bronze
    df_ecom_bronze = spark.table("smartdata.bronze.ecommerce_raw")

    # Aplicar transformaciones
    df_ecom_silver = transform_ecommerce_silver(df_ecom_bronze)

    print("⚙️ Transformaciones Silver aplicadas correctamente.")

    # Guardar tabla Silver en Delta
    df_ecom_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("smartdata.silver.ecommerce_silver")

    print("🥈 Tabla Silver creada: smartdata.silver.ecommerce_silver")
    print("🚀 Proceso SILVER finalizado correctamente.")


if __name__ == "__main__":
    main()
