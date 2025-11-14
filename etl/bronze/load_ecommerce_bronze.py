from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =========================================================
#  BRONZE - Carga de datos RAW (Ecommerce)
# =========================================================

spark = SparkSession.builder.getOrCreate()

# Seleccionamos catálogo y schema Bronze
spark.sql("USE CATALOG smartdata")
spark.sql("USE SCHEMA bronze")

# Ruta del archivo (ADLS)
input_path = "abfss://bronze@adlssmartdatacesar.dfs.core.windows.net/Ecommerce_Sales_Prediction_Dataset.csv"

print("📂 Leyendo archivo RAW desde:", input_path)

# Cargar datos RAW
df_ecom_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(input_path)
)

print("✅ Registros cargados en Bronze (Ecommerce):", df_ecom_bronze.count())

# Guardar como Delta
df_ecom_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("smartdata.bronze.ecommerce_raw")

print("🟫 Tabla Bronze creada: smartdata.bronze.ecommerce_raw")
print("🚀 Proceso BRONZE finalizado correctamente.")
