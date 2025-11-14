# etl/gold/generate_gold_sales_marketing.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def generate_gold(df_mkt, df_ecom):
    """
    Combina marketing y ecommerce para generar la capa GOLD.
    
    Join usado:
    df_ecom.Customer_Segment == df_mkt.Marital_Status
    
    Tipo de unión: INNER
    """
    df_gold = df_ecom.join(
        df_mkt,
        df_ecom["Customer_Segment"] == df_mkt["Marital_Status"],
        "inner"
    )
    return df_gold


def main():
    # Crear Spark session
    spark = SparkSession.builder.getOrCreate()

    # Seleccionar catálogo y esquema
    spark.sql("USE CATALOG smartdata")
    spark.sql("USE SCHEMA gold")

    # Leer tablas desde SILVER
    df_mkt = spark.table("smartdata.silver.marketing_silver")
    df_ecom = spark.table("smartdata.silver.ecommerce_silver")

    # Generar GOLD
    df_gold = generate_gold(df_mkt, df_ecom)

    # Guardar GOLD como tabla Delta
    df_gold.write.format("delta").mode("overwrite") \
        .saveAsTable("smartdata.gold.sales_marketing_gold")

    print("✔ GOLD generado y almacenado correctamente.")


if __name__ == "__main__":
    main()
