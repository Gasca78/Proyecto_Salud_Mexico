# 🏥 Serverless Data Engineering Pipeline: Análisis de Salud Pública Cloud
### Monitoreo de Enfermedades Cardiovasculares en México (AWS Nativo)

![AWS S3](https://img.shields.io/badge/AWS-S3_Data_Lake-red?logo=amazons3)
![AWS Lambda](https://img.shields.io/badge/AWS-Lambda_Event_Driven-orange?logo=awslambda)
![AWS Glue](https://img.shields.io/badge/AWS-Glue_Serverless_Spark-orange?logo=amazonaws)
![Amazon Athena](https://img.shields.io/badge/Amazon-Athena_SQL-blue?logo=amazonaws)
![PowerBI](https://img.shields.io/badge/PowerBI-DirectQuery-yellow?logo=powerbi)

## 📌 Descripción del Proyecto
Este proyecto consiste en el diseño e implementación de un pipeline de datos **End-to-End (E2E) y Serverless** en la nube de AWS. Procesa, limpia y consolida más de 10 años de datos históricos (2014-2024) de mortalidad y egresos hospitalarios por afecciones cardiovasculares en México, permitiendo análisis epidemiológicos de gran escala mediante métricas estandarizadas (Tasas por cada 100,000 habitantes).

## 🏗️ Arquitectura de Datos y Flujo Cloud (Medallion Architecture)
La solución implementa un patrón moderno de desacoplamiento de almacenamiento y cómputo (*Storage vs. Compute*) estructurado bajo la arquitectura Medallion, orquestado mediante una arquitectura **Event-Driven**:

```mermaid
graph LR
    A[Fuentes Gov: INEGI/DGIS/CONAPO] -->|Extracción e Ingesta| B(Amazon S3: bronze/)
    B -->|AWS Glue Job: PySpark Silver| C(Amazon S3: silver/)
    C -->|AWS Glue Job: PySpark Gold| D(Amazon S3: gold/ particionado)
    D -->|Glue Crawler| E[AWS Glue Data Catalog]
    E --- F[Amazon Athena]
    F -->|DirectQuery / Simba ODBC| G[Power BI Dashboard Live]
```

1. **Capa Bronze (Almacenamiento de Objetos S3):** Depósito centralizado de archivos históricos `.csv` extraídos de portales gubernamentales. La llegada de un nuevo archivo dispara automáticamente un evento `s3:ObjectCreated:*`.
2. **Orquestación Event-Driven (AWS Lambda + Glue Triggers):** Una función Lambda intercepta el evento de S3 e inicia de manera asíncrona el pipeline de transformación. Los pasos subsecuentes se encadenan de forma nativa mediante Glue Event Triggers, previniendo *timeouts* y optimizando costos de cómputo.
3. **Capa Silver (AWS Glue ETL - PySpark):** Procesamiento distribuido serverless optimizado a 2 Workers (G.1X). Implementa limpieza avanzada, tipado estricto, imputación de nulos, homogeneización de formatos de fecha asimétricos y mapeo de diccionarios médicos internacionales (CIE-10). Datos persistidos en formato comprimido **Apache Parquet**.
4. **Capa Gold (Modelado Analítico de Negocio):** Consolidación automatizada mediante un *Mega Join* analítico de los tres universos (Población, Mortalidad, Egresos). Los datos son **particionados físicamente por Año y Estado** para maximizar la velocidad de lectura (`Partition Pruning`).

## 🚀 Desafíos Técnicos Cloud Resueltos
* **Evasión de JAR Hell y Conflictos de Red de la JVM:** Se implementó un desacoplamiento de red utilizando la API de Spark nativa de AWS en entornos administrados, eliminando las dependencias y latencias de protocolos externos en la nube de Amazon.
* **Manejo de Errores de Versión de Motores (Spark 3.3 vs 3.4):** Se resolvieron incompatibilidades funcionales (*Unresolved Routines*) adaptando expresiones analíticas estrictas a APIs nativas de columnas compatibles con entornos Serverless estables (AWS Glue 4.0), reemplazando `try_to_date` por cascadas controladas de `F.coalesce` y `F.to_date` tolerantes a milisegundos anómalos (`.SSS`).
* **Bypass de la división entera en Motores SQL ANSI (Amazon Athena):** Se corrigió la pérdida de precisión decimal (`QUERY_ERROR`) forzando conversiones de tipo explícitas (`CAST AS DOUBLE`) y multiplicaciones flotantes en consultas SQL agregadas para prevenir tasas de mortalidad en cero.
* **Seguridad y Acceso Basado en Roles (IAM):** Configuración de políticas de menor privilegio (`AmazonAthenaFullAccess`, `AmazonS3FullAccess`, `AWSLambdaBasicExecutionRole`) para establecer túneles de comunicación seguros y ejecuciones orquestadas.

## 📊 Dashboard y Resultados
El producto final es un panel estratégico de salud pública conectado en vivo (*DirectQuery*) mediante ODBC a Amazon Athena, lo que permite:
* Identificar zonas de vulnerabilidad epidemiológica extrema mediante correlación geográfica.
* Evaluar el impacto real de la saturación hospitalaria frente al incremento de muertes por causas cardiovasculares durante el periodo de pandemia (2020-2021).
* Analizar de manera dinámica micro-segmentos demográficos sin latencia, gracias a la partición de datos en el Data Lake.

![Dashboard de Salud](https://github.com/Gasca78/Proyecto_Salud_Mexico/blob/main/dashboard/Panel_1.png)
![Dashboard de Salud](https://github.com/Gasca78/Proyecto_Salud_Mexico/blob/main/dashboard/Panel_2.png)
![Demo del Dashboard interactivo](https://github.com/Gasca78/Proyecto_Salud_Mexico/blob/main/dashboard/dashboard_gif.gif)

## ⚙️ Estructura del Repositorio
* `src/pipeline_silver.py`: Script de PySpark ejecutado en AWS Glue para la transformación de capas limpias.
* `src/pipeline_gold.py`: Script analítico para agregaciones, cálculos matemáticos de tasas y particionamiento en S3.
* `automation/lambda_trigger.py`: Función *serverless* que sirve como punto de entrada (Event Handler) para la orquestación continua del pipeline.
