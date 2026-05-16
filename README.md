# 🏥 Data Engineering Pipeline: Análisis de Salud Pública (Enfermedades Cardiovasculares en México)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![PySpark](https://img.shields.io/badge/PySpark-Data_Processing-orange)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)
![Status](https://img.shields.io/badge/Status-Completed-success)

## 📌 Descripción del Proyecto
Este proyecto es un pipeline de datos **End-to-End (E2E)** diseñado para procesar, limpiar y analizar más de 10 años de datos históricos (2014-2024) sobre mortalidad y egresos hospitalarios por afecciones cardiovasculares en México. 

El objetivo principal fue construir un Data Warehouse estructurado y optimizado que permita a los tomadores de decisiones visualizar el riesgo epidemiológico a nivel nacional, estandarizando las métricas (Tasa por cada 100,000 habitantes) para comparaciones demográficas justas.

## 🏗️ Arquitectura de Datos (Medallion Architecture)
El proyecto sigue la arquitectura Medallion (Bronze, Silver, Gold) utilizando **PySpark** para el procesamiento distribuido:

1. **Capa Bronze (Raw):** Ingesta de archivos `.csv` y `.zip` extraídos mediante Web Scraping (Requests/BeautifulSoup) con bypass de medidas Anti-Bot desde portales gubernamentales (INEGI, DGIS, CONAPO).
2. **Capa Silver (Cleansed):** Estandarización de columnas, imputación de nulos, formateo de fechas, mapeo de catálogos (CIE-10) y normalización de llaves asimétricas (ej. formateo `00-04` vs `0-4`). Datos guardados en formato `.parquet`.
3. **Capa Gold (Business-level):** Construcción de la Tabla de Hechos (`fact_dataset.parquet`) mediante un *Full Outer Join* de las dimensiones de población, mortalidad y egresos.

## 🚀 Desafíos Técnicos Resueltos
* **Prevención de Out-of-Memory (OOM) y Cartesian Joins:** Se optimizó el motor de Spark limitando las particiones de *shuffle* (`spark.sql.shuffle.partitions = 8`) y materializando dataframes en memoria (`.cache()`) tras realizar agrupaciones (`groupBy`) previas a los cruces masivos.
* **Resolución de Data Swamps:** Gestión de metadatos y control de versiones en sistemas de archivos locales/Drive para evitar la lectura de *ghost partitions* en archivos Parquet fragmentados.
* **Cálculo DAX Dinámico:** Implementación de medidas para evitar la suma lineal de tasas, calculando iterativamente `DIVIDE(SUM(Muertes) * 100000, SUM(Poblacion))` según el contexto del filtro en el dashboard.

## 📊 Dashboard y Resultados
El producto final es un dashboard interactivo en Power BI que revela:
* La curva biológica de riesgo cardiovascular (pico en el rango de 75-84 años).
* El impacto histórico de la pandemia (2020-2021) en la saturación hospitalaria y mortalidad.
* Distribución geográfica del riesgo epidemiológico mediante un mapa de formas.

![Dashboard de Salud](https://github.com/Gasca78/Proyecto_Salud_Mexico/blob/main/dashboard/Panel_1.png)
![Dashboard de Salud](https://github.com/Gasca78/Proyecto_Salud_Mexico/blob/main/dashboard/Panel_2.png)
![Demo del Dashboard interactivo](https://github.com/Gasca78/Proyecto_Salud_Mexico/blob/main/dashboard/dashboard_gif.gif)

## ⚙️ Cómo ejecutar este proyecto
1. Clonar el repositorio: `git clone [https://github.com/Gasca78/Proyecto_Salud_Mexico/]`
2. Instalar dependencias: `pip install -r requirements.txt`
3. Ejecutar las capas en orden:
   ```bash
   python src/pipeline_silver.py
   python src/03_gold_fact_salud.py
