# project_medallion_databricks
Proyecto Final – Arquitectura Medallion con Azure + Databricks

Este proyecto implementa una arquitectura Medallion (Bronze → Silver → Gold) utilizando Azure Data Lake, Azure Databricks, Delta Lake y PySpark.

Incluye:

Ingesta de datos RAW (Bronze)

Transformación y limpieza robusta (Silver)

Modelo final analítico unificado (Gold)

Dashboard embebido con Databricks

Código organizado y publicado en GitHub

✔️ Arquitectura
Bronze → Silver → Gold → Dashboard


Bronze: Datos crudos, sin transformar.

Silver: Estandarización, limpieza, cast, null handling, enriquecimiento.

Gold: Unificación Marketing + Ecommerce para analítica.

✔️ ETL Implementada
🟫 Bronze

Carga directa desde CSV a tablas Delta.

Se crean:

smartdata.bronze.marketing_campaign_raw

smartdata.bronze.ecommerce_raw

🟪 Silver

Limpieza completa:

cast de tipos

normalización

eliminación de duplicados

creación de columnas derivadas como:

Total_Spending

Age

Income_Level

🟨 Gold

Unión completa entre Marketing y Ecommerce

Cálculo de KPIs

Creación de tabla final para dashboard:

smartdata.gold.sales_marketing_gold

✔️ Dataframes Resultantes
Tabla GOLD contiene:

2.216.000 registros

39 columnas limpias listas para analítica

Segmentos: Premium, Regular, Occasional

Categorías: Fashion, Sports, Electronics, Home Decor, Toys

📊 Dashboard

Incluye 3 visualizaciones:

Ventas por Categoría (bar chart)

Marketing Spend por Segmento

Compras Web vs Tienda

Todas construidas directamente sobre la tabla GOLD.

📁 Estructura del Repositorio
etl/           → scripts de ETL Bronze/Silver/Gold
notebooks/     → notebook principal del proyecto
dashboards/    → capturas de las visualizaciones
scripts/       → SQL de catálogo y permisos

👨‍💻 Autor

César Fernando Campos Millán
Especialista en Big Data | Arquitectura de Datos | PySpark | Azure | Databricks
