import os
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F

# Basic logger setting 
logging.basicConfig(
    filename='pipeline_gold.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# spark_engine Engine
conf = SparkConf()
conf.set('spark.executor.memory', '4g')
conf.set('spark.driver.memory', '4g')
conf.set('spark.sql.shuffle.partitions', '8')

def start_spark_engine():
    logging.info('Encendiendo el motor de spark_engine...')
    spark_engine = SparkSession.builder \
        .appName('PipelineSalud_Gold') \
        .config(conf=conf) \
        .getOrCreate()
    return spark_engine

# Function to join the datasets and obtain a "master" dataset 
def final_df(data_path):
    # Define paths
    silver_path = os.path.join(data_path, 'silver')
    mortalidad_path = os.path.join(silver_path, 'mortalidad_inegi.parquet')
    egresos_path = os.path.join(silver_path, 'egresos_dgis.parquet')
    poblacion_path = os.path.join(silver_path, 'poblacion_conapo.parquet')
    gold_path = os.path.join(data_path, 'gold', 'fact_dataset.parquet')
    
    logging.info(f'Reading data from {silver_path}')
    
    # 1. Reading all the files and save in diferent DataFrames:
    df_mortalidad = spark_engine.read.parquet(mortalidad_path)
    df_egresos = spark_engine.read.parquet(egresos_path)
    df_poblacion = spark_engine.read.parquet(poblacion_path)
    
    # 2. Normalize information (name columns)
    # Mortality
    map_mortalidad = {'ENT_RESID': 'ID_ESTADO', 'NOMBRE_ENTIDADES_RESIDENCIA': 'ESTADO', 'ANIO_OCUR': 'ANIO_ANALISIS'}
    df_mortalidad = df_mortalidad.withColumnsRenamed(map_mortalidad)
    # Poblation
    map_poblacion = {'CLAVE_ENT': 'ID_ESTADO', 'NOMBRE_ENTIDADES': 'ESTADO', 'ANO': 'ANIO_ANALISIS'}
    df_poblacion = df_poblacion.withColumnsRenamed(map_poblacion)
    
    # 3. Filter df_poblacion to get only the years we need (2014-2024) and keep only males and females
    filter_age = ((F.col('ANIO_ANALISIS') >= 2014) & (F.col('ANIO_ANALISIS') <= 2024))
    filter_sex = F.col('SEXO').isin('HOMBRES', 'MUJERES')
    df_poblacion = df_poblacion.filter(filter_age & filter_sex)
    df_egresos = df_egresos.filter(filter_age & filter_sex)
    df_mortalidad = df_mortalidad.filter(filter_age & filter_sex)
    
    # 3.5 Make sure dataframes are correctly grouping
    # Collapse df_mortalidad
    df_mortalidad = df_mortalidad.groupBy('ID_ESTADO', 'ESTADO', 'ANIO_ANALISIS', 'SEXO', 'RANGO_EDAD').agg(F.sum('TOTAL_MUERTES').alias('TOTAL_MUERTES'))
    #Collapse df_poblacion
    df_poblacion = df_poblacion.groupBy('ID_ESTADO', 'ESTADO', 'ANIO_ANALISIS', 'SEXO', 'RANGO_EDAD').agg(F.sum('POBLACION_TOTAL').alias('POBLACION_TOTAL'))
    # Collapse df_egresos
    df_egresos = df_egresos.groupBy('ID_ESTADO', 'ESTADO', 'ANIO_ANALISIS', 'SEXO', 'RANGO_EDAD').agg(F.sum('TOTAL_EGRESOS').alias('TOTAL_EGRESOS'))
    # Storage in RAM to optimize process
    df_mortalidad.cache()
    df_poblacion.cache()
    df_egresos.cache() 
    # Force work 
    df_mortalidad.count()
    df_poblacion.count()
    df_egresos.count()
    
    # 4. Make the MEGA JOIN
    key_join = ['ID_ESTADO', 'ESTADO', 'ANIO_ANALISIS', 'SEXO', 'RANGO_EDAD']
    df_fact = df_mortalidad.join(df_poblacion, on=key_join, how='full') \
        .join(df_egresos, on=key_join, how='full')
    
    # 5. Fill files with zero where are null's
    df_fact = df_fact.fillna({'TOTAL_MUERTES': 0, 'TOTAL_EGRESOS': 0})
    
    # 6. Add calculated columns 
    df_fact = df_fact.withColumn('TASA_MORTALIDAD_100K', F.round(F.col('TOTAL_MUERTES')/F.col('POBLACION_TOTAL')*100000,2))
    df_fact = df_fact.withColumn('TASA_EGRESOS_100K', F.round(F.col('TOTAL_EGRESOS')/F.col('POBLACION_TOTAL')*100000,2))
    
    # 7. Save the file
    logging.info('Guardando el dataset en formato Parquet...')
    df_fact.write.mode('overwrite').parquet(gold_path)
    logging.info(f'¡Éxito! Datos guardados en: {gold_path}')

if __name__=="__main__":
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.join(BASE_PATH, '..', 'data')
    
    # Create gold folder if not exists
    os.makedirs(os.path.join(DATA_PATH, 'gold'), exist_ok=True)
    
    spark_engine = start_spark_engine()
    
    try:
        final_df(DATA_PATH)
    except Exception as e:
        logging.error(f'Failure Gold layer: {e}')
    finally:
        spark_engine.stop()
        logging.info('spark_engine Engine off')