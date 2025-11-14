🏗️ Proyecto Final – Arquitectura Medallion con Azure + Databricks

Autor: César Fernando Campos Millán
Curso: Ingeniería de Datos e IA con Databricks
Fecha: Noviembre 2025

Este proyecto implementa un flujo completo de ingesta, transformación y analítica utilizando la Arquitectura Medallion (Bronze → Silver → Gold) sobre Azure Databricks y Azure Data Lake Storage (ADLS).
Incluye:

ETL en PySpark

Tablas Delta en cada capa

Orquestación mediante Workflow (Job)

Dashboards analíticos en Databricks

🚀 Arquitectura General del Proyecto

El flujo de datos sigue el estándar de la arquitectura Medallion:

RAW → BRONZE → SILVER → GOLD → Dashboards

Tecnologías utilizadas:

Azure Data Lake Storage (ADLS)

Azure Databricks

PySpark

Delta Lake

Unity Catalog

Databricks SQL Dashboards

Workflows (Jobs)

🥉 Capa BRONZE – Ingesta de Datos Crudos

En esta capa se cargaron los datasets originales:

marketing_campaign.csv (desde DBFS)

Ecommerce_Sales_Prediction_Dataset.csv (desde ADLS)

Acciones realizadas:

Lectura en formato CSV

Inferencia de esquema

Normalización básica de columnas

Almacenamiento en Delta Lake

Tablas generadas:

smartdata.bronze.marketing_raw

smartdata.bronze.ecommerce_raw

🥈 Capa SILVER – Limpieza y Transformación

Acciones realizadas:

Conversión de tipos de datos (fechas, enteros, double)

Estandarización de columnas

Preparación de datos para la capa analítica

Tablas generadas:

smartdata.silver.marketing_campaign_silver

smartdata.silver.ecommerce_silver

🥇 Capa GOLD – Enriquecimiento Analítico

En GOLD se generan métricas, agregaciones y nuevas columnas:

Nuevas variables:

Age

Total_Spend

Net_Price

Revenue

Se creó una tabla final unificada:

smartdata.gold.sales_marketing_gold

Esta tabla combina información de marketing y ventas para análisis avanzados.

⚙️ Workflow (Job) del Proyecto

Se configuró un Job/Workflow llamado:

job_medallion_etl_full

Características:

Ejecuta el notebook del ETL completo

Usa el cluster smartdata_cluster_uc

Procesa Bronze → Silver → Gold automáticamente

Validado exitosamente (Runs en verde)

Capturas incluidas en el repositorio:

Ejecución (Runs)

Definición de tareas (Tasks)

📊 Dashboards del Proyecto

Se generaron 3 dashboards en Databricks SQL:

📌 1. Ventas por Categoría

Muestra ingresos por tipo de producto.

📌 2. Marketing Spend por Segmento

Analiza gasto en marketing por segmento de cliente.

📌 3. Compras Web vs Tienda

Comparación de canales y comportamiento del consumidor.

Imágenes adjuntas en la carpeta /dashboards.

📝 Resultados y Conclusiones

Arquitectura Medallion implementada de forma correcta

ETL funcionando end-to-end

Tablas Delta creadas correctamente bajo Unity Catalog

Dashboards funcionales para análisis de negocio

Workflow ejecutado con éxito (estado: Succeeded)

Proyecto listo para presentación y evaluación

📫 Contacto

César Fernando Campos Millán
Especialista en Big Data y Analítica
