from pyspark.sql import SparkSession

# =========================================================
#  BRONZE - Carga de datos RAW (Marketing)
# =========================================================

spark = SparkSession.builder.getOrCreate()

# Seleccionamos catálogo y schema Bronze
spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA bronze")

# Ruta del archivo RAW
input_path = "dbfs:/FileStore/marketing_campaign.csv"

print("📂 Leyendo archivo RAW desde:", input_path)

# Cargar archivo CSV
df_mkt_bronze = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")  # detecta tipos automáticamente
        .load(input_path)
)

# Validación básica
print("✅ Registros cargados en Bronze (Marketing):", df_mkt_bronze.count())

# Guardar como Delta
df_mkt_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata.bronze.marketing_raw")

print("🟫 Tabla
