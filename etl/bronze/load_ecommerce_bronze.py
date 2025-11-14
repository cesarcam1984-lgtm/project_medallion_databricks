from pyspark.sql import SparkSession

# =========================================================
#  BRONZE - Carga de datos RAW (Ecommerce)
# =========================================================

spark = SparkSession.builder.getOrCreate()

# Seleccionamos catálogo y schema Bronze
spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA bronze")

# Ruta del archivo
input_path = "dbfs:/FileStore/ecommerce_data.csv"

print("📂 Leyendo archivo RAW desde:", input_path)

# Cargar datos RAW
df_ecom_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")   # detectar tipos automáticamente
    .load(input_path)
)

# Verificación básica
print("✅ Registros cargados en Bronze (Ecommerce):", df_ecom_bronze.count())

# Guardar en formato Delta
df_ecom_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata.bronze.ecommerce_raw")

print("🟫 Tabla Bronze creada: smartdata.bronze.ecommerce_raw")
print("🚀 Proceso BRONZE finalizado correctamente.")
