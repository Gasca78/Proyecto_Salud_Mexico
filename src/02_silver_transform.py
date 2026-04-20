import os
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from functools import reduce

# Basic logger setting 
logging.basicConfig(
    filename='pipeline_silver.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# spark_engine Engine
def start_spark_engine():
    logging.info('Encendiendo el motor de Pyspark_engine...')
    spark_engine = SparkSession.builder \
        .appName('PipelineSalud_Silver') \
        .config('spark_engine.driver.memory', '4g') \
        .getOrCreate()
    return spark_engine

# General functions
'''
Function to get a datetime column using columns with dates.
'''
def date_transform(df, day_col, month_col, year_col, new_column='Date', erase_day=False):
    date_string =  F.concat_ws("-", # With concat_ws concatenated strings with an separator
                               F.col(year_col),
                               F.lpad(F.col(month_col), 2, '0'), # lpad fill with zero at left
                               F.lpad(F.col(day_col), 2, '0'))
    df = df.withColumn(new_column, F.to_date(date_string)) 
    if erase_day:
        df = df.drop(day_col, month_col)
    return df
'''
Function to transform into null any values like 99, 999, 9999
'''
def institutional_null_clean(df, inst_col):
  replace_values = [99, '99', 999, '999', 9999, '9999']
  for column in inst_col:
    df = df.withColumn(column, F.when(F.col(column).isin(replace_values), F.lit(None)).otherwise(F.col(column)))
  return df
'''
Funtion to join the dataframe with different catalogs
'''
def add_description(df, df_cat, code_col, union_code, description_col, new_column=f'DESCRIPCION'):
  df_cat = df_cat.withColumnRenamed(description_col, new_column)
  df = df.join(df_cat, on=df[code_col]==df_cat[union_code], how='left')
  df = df.drop(df_cat.columns[0])
  return df
'''
Funtion to add a column with the age discretized
'''
def age_discretize(df, age_col, new_column='RANGO_EDAD'):
  df = df.withColumn(new_column,
                     F.when(F.col(age_col) < 5, '0-4')
                     .when(F.col(age_col) < 10, '5-9')
                     .when(F.col(age_col) < 15, '10-14')
                     .when(F.col(age_col) < 20, '15-19')
                     .when(F.col(age_col) < 25, '20-24')
                     .when(F.col(age_col) < 30, '25-29')
                     .when(F.col(age_col) < 35, '30-34')
                     .when(F.col(age_col) < 40, '35-39')
                     .when(F.col(age_col) < 45, '40-44')
                     .when(F.col(age_col) < 50, '45-49')
                     .when(F.col(age_col) < 55, '50-54')
                     .when(F.col(age_col) < 60, '55-59')
                     .when(F.col(age_col) < 65, '60-64')
                     .when(F.col(age_col) < 70, '65-69')
                     .when(F.col(age_col) < 75, '70-74')
                     .when(F.col(age_col) < 80, '75-79')
                     .when(F.col(age_col) < 85, '80-84')
                     .otherwise('85+'))
  return df
'''
Special funtion to hospital discharges files
'''
def smart_reader_discharges(file_path):
  # Extract the extension and turn into lowercase
  _, extension = os.path.splitext(file_path)
  extension = extension.lower()
  
  ###############################
  # If extension is an excel file
  ###############################
  if extension in ['.xlsx', '.xls']:
    df = spark_engine.read.csv(file_path, header=True, inferSchema=True)
  ###############################
  # If extension is a text file
  ###############################
  else:
    # Check the first line to find the separator
    with open(file_path, 'r', encoding='iso-8859-1') as f:
      first_line = f.readline()
    separator = '|' if '|' in first_line else ','
    df = spark_engine.read.csv(file_path, header=True, inferSchema=False, sep=separator)

  # Standardized all the columns to capital
  df = df.toDF(*[c.upper() for c in df.columns])
  return df

def mortality_process(data_path):
  # Define paths
  bronze_path = os.path.join(data_path, 'bronze', 'mortalidad_inegi_20260408.csv')
  silver_path_pp = os.path.join(data_path, 'silver', 'mortalidad_inegi_per_people.parquet')
  silver_path = os.path.join(data_path, 'silver', 'mortalidad_inegi.parquet')
  
  logging.info(f'Reading data from: {bronze_path}')
  
  # Read the file
  df = spark_engine.read.csv(bronze_path, header=True, inferSchema=True, sep=',')
  
  # Processing
  
  # 1. Capitalize columns names
  df = df.toDF(*[c.upper() for c in df.columns])
  
  # 2. Filter by heart deseases
  df_filtered = df.filter(F.col('CAUSA_DEF').startswith('I'))
  
  # 3. Select only the required columns
  df_selected = df_filtered.select('ENT_RESID', 'CAUSA_DEF', 'SEXO', 'EDAD', 'DIA_OCURR', 'MES_OCURR', 'ANIO_OCUR', 'DIA_NACIM', 'MES_NACIM', 'ANIO_NACIM', 'TIPO_DEFUN')
  
  # 4. Nulls treatment
  df_selected = institutional_null_clean(df_selected, ['DIA_OCURR', 'MES_OCURR', 'ANIO_OCUR', 'DIA_NACIM', 'MES_NACIM', 'ANIO_NACIM'])
  df_limpio = df_selected.dropna()
  
  # 4. Get a date column 
  df = date_transform(df_limpio, 'DIA_OCURR', 'MES_OCURR', 'ANIO_OCUR', new_column='FECHA_OCURR', erase_day=True)
  df = date_transform(df, 'DIA_NACIM', 'MES_NACIM', 'ANIO_NACIM', new_column='FECHA_NACIM', erase_day=True)
  
  # 5. Add descriptions
  # Desiese description (CIE10)
  df_cat_cie10 = cat_cie10.select('CLAVE', 'NOMBRE')
  df = add_description(df, df_cat_cie10, code_col='CAUSA_DEF', union_code='CLAVE', description_col='NOMBRE', new_column='DESCRIPCION_CAUSA')
  # Death description
  df = add_description(df, cat_tipo_def, code_col='TIPO_DEFUN', union_code='CVE', description_col='DESCRIP', new_column='DESCRIPCION_TIPO_DEF')
  # Entity residence
  df = df.filter(F.col('ENT_RESID') <= 32)
  df = add_description(df, cat_entidades, code_col='ENT_RESID', union_code='EDO', description_col='DESCRIP', new_column='NOMBRE_ENTIDADES_RESIDENCIA')
  
  # 6. Fix Ages
  df = df.withColumn('EDAD',
                      F.when(F.col('EDAD').cast('string').startswith('4'), F.col('EDAD')-4000)
                      .when(F.col('EDAD') > 120, F.lit(None))
                      .otherwise(0))
  
  # 7. Change Sex number to description
  df = df.withColumn('SEXO',
                      F.when(F.col('SEXO') == 1, 'HOMBRES')
                      .when(F.col('SEXO') == 2, 'MUJERES')
                      .otherwise('DESCONOCIDO'))
  
  # 8. Reorder dataframe columns
  df = df.select('ENT_RESID', 'NOMBRE_ENTIDADES_RESIDENCIA', 'ANIO_OCUR', 'FECHA_OCURR', 'FECHA_NACIM', 'SEXO', 'EDAD', 'CAUSA_DEF', 'DESCRIPCION_CAUSA', 'TIPO_DEFUN', 'DESCRIPCION_TIPO_DEF')
  
  # 9. Aggrupation
  df_agg = age_discretize(df, age_col='EDAD', new_column='RANGO_EDAD')
  df_agg = df_agg.filter((F.col('EDAD') >= 2015) & (F.col('EDAD') <= 2024))
  df_agg = df_agg.groupBy('ENT_RESID', 'NOMBRE_ENTIDADES_RESIDENCIA', 'ANIO_OCUR', 'SEXO', 'RANGO_EDAD').agg(F.count('*').alias('TOTAL_MUERTES'))
  
  # 10. Save the files
  logging.info("Guardando los datos transformados en formato Parquet...")
  df.write.mode("overwrite").parquet(silver_path_pp)
  df_agg.write.mode("overwrite").parquet(silver_path)
  logging.info(f"¡Éxito! Datos guardados en: {silver_path}")
    
def population_process(data_path):
  # Define paths
  bronze_path = os.path.join(data_path, 'bronze', 'poblacion_conapo_20260408.csv')
  silver_path_pp = os.path.join(data_path, 'silver', 'poblacion_conapo_total_population.parquet')
  silver_path = os.path.join(data_path, 'silver', 'poblacion_conapo.parquet')
  
  logging.info(f'Reading data from: {bronze_path}')
  
  # Read the file
  df = spark_engine.read.csv(bronze_path, header=True, inferSchema=True, sep=',')
  
  # Processing
  
  # 1. Capitalize columns names
  df = df.toDF(*[c.upper() for c in df.columns])
  
  # 2. Select only the required columns
  df_selected = df.drop('CLAVE', 'NOM_MUN', 'ETIQUETA_ESTADO', 'FECHA')
  
  # 3. Add descriptions
  # Entity residence
  df = df_selected.filter(F.col('CLAVE_ENT') <= 32)
  df = add_description(df, cat_entidades, code_col='CLAVE_ENT', union_code='EDO', description_col='DESCRIP', new_column='NOMBRE_ENTIDADES')
  df = df.drop('NOM_ENT')
  
  # 4. Aggrupation
  df_agg = df.groupBy('CLAVE_ENT', 'NOMBRE_ENTIDADES', 'ANO', 'SEXO').agg(
    F.sum('POB_TOTAL').alias('POB_TOTAL'),
    F.sum('POB_00_04').alias('0-4'),
    F.sum('POB_05_09').alias('5-9'),
    F.sum('POB_010_014').alias('10-14'),
    F.sum('POB_015_019').alias('15-19'),
    F.sum('POB_20_24').alias('20-24'),
    F.sum('POB_25_29').alias('25-29'),
    F.sum('POB_30_34').alias('30-34'),
    F.sum('POB_35_39').alias('35-39'),
    F.sum('POB_40_44').alias('40-44'),
    F.sum('POB_45_49').alias('45-49'),
    F.sum('POB_50_54').alias('50-54'),
    F.sum('POB_55_59').alias('55-59'),
    F.sum('POB_60_64').alias('60-64'),
    F.sum('POB_65_69').alias('65-69'),
    F.sum('POB_70_74').alias('70-74'),
    F.sum('POB_75_79').alias('75-79'),
    F.sum('POB_80_84').alias('80-84'),
    F.sum('POB_85_MM').alias('85+')
).orderBy('CLAVE_ENT', 'ANO', 'SEXO')
  
  # 5. Separate an file with only total population
  df_total = df_agg.select('CLAVE_ENT', 'NOMBRE_ENTIDADES', 'ANO', 'SEXO', 'POB_TOTAL')
  
  # 6. Melted df to age columns transform into rows
  age_columns = df_agg.drop('CLAVE_ENT', 'NOMBRE_ENTIDADES', 'ANO', 'SEXO', 'POB_TOTAL').columns
  fixed_columns = ['CLAVE_ENT', 'NOMBRE_ENTIDADES', 'ANO', 'SEXO']
  df_population = df_agg.unpivot(
    ids=fixed_columns,
    values=age_columns,
    variableColumnName='RANGO_EDAD',
    valueColumnName='POBLACION_TOTAL'
  )
  
  # 7. Save the files
  logging.info("Guardando los datos transformados en formato Parquet...")
  df_total.write.mode("overwrite").parquet(silver_path_pp)
  df_population.write.mode("overwrite").parquet(silver_path)
  logging.info(f"¡Éxito! Datos guardados en: {silver_path}")
  
def hospital_discharge_process(data_path):
  # Define paths
  bronze_path = os.path.join(data_path, 'bronze')
  silver_path_pp = os.path.join(data_path, 'silver', 'egresos_dgis_per_people.parquet')
  silver_path = os.path.join(data_path, 'silver', 'egresos_dgis.parquet')

  logging.info(f'Reading data from: {bronze_path}')

  # 1. Reading all the files
  files_list = os.listdir(bronze_path)
  df_list = []
  for file in files_list:
    if 'egreso' in file:
      # Reading the file
      df_temp = smart_reader_discharges(os.path.join(bronze_path, file))
      # Date cleaning
      date_columns = ['EGRESO', 'INGRE']
      for date_col in date_columns:
        df_temp = df_temp.withColumn(date_col, F.col(date_col).cast('string'))
        # Format waterfall with try_to_date
        df_temp = df_temp.withColumn(date_col, F.coalesce(
                  F.expr(f"try_to_date({date_col}, 'yyyy-MM-dd HH:mm:ss')"),
                  F.expr(f"try_to_date({date_col}, 'yyyy-MM-dd')"),
                  F.expr(f"try_to_date({date_col}, 'dd/MM/yyyy HH:mm')"),
                  F.expr(f"try_to_date({date_col}, 'dd/MM/yyyy')"),
                  F.expr(f"try_to_date({date_col}, 'MM/dd/yyyy HH:mm')"),
                  F.expr(f"try_to_date({date_col}, 'MM/dd/yyyy')")
              ))
      # Filter by heart diseases
      df_temp = df_temp.filter(F.col('AFECPRIN').startswith('I')) 
      # Save in files_list
      df_list.append(df_temp)
  df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)
  
  # 2. Select only the required columns
  df = df.select('ID', 'EGRESO', 'INGRE', 'DIAS_ESTA', 'CVEEDAD', 'EDAD', 'NACIOEN', 'SEXO', 'ENTIDAD', 'MOTEGRE', 'AFECPRIN', 'VEZ', 'MES_ESTADISTICO')
  
  # 3. Add new column 'ANIO' for only the year and cast the columns that not are strings
  df = df.withColumn('ANIO', F.year(F.col('EGRESO')))
  columns = ['ID', 'DIAS_ESTA', 'CVEEDAD', 'EDAD', 'ENTIDAD', 'VEZ', 'MES_ESTADISTICO']
  for col in columns:
    df = df.withColumn(col, F.expr(f'try_cast({col} AS int)'))
  df = df.dropna(subset=['ENTIDAD', 'EDAD', 'ANIO', 'EGRESO', 'INGRE'])
  
  # 4. Age to only years
  df = df.withColumn('EDAD',
                     F.when(F.col('CVEEDAD') == 3, F.col('EDAD'))
                     .otherwise(0))
  df = df.drop('CVEEDAD')
  
  # 5. Filter to entity residence only in Mexico
  df = df.filter(F.col('ENTIDAD').between(1, 32))
  
  # 6. Add description
  df_cat_cie10 = cat_cie10.select('CLAVE', 'NOMBRE')
  # CIE10
  df = add_description(df, df_cat_cie10, code_col='AFECPRIN', union_code='CLAVE', description_col='NOMBRE', new_column='DESCRIPCION_CAUSA')
  # NACIOEN
  df = add_description(df, cat_nacioen, code_col='NACIOEN', union_code='IdSINO', description_col='Descrip', new_column='NACIO_EN_HOSPITAL')
  df = df.drop('NACIOEN')
  # SEXO
  df = add_description(df, cat_sexo, code_col='SEXO', union_code='IdSexo', description_col='Descrip', new_column='SEXO_')
  df = df.drop('SEXO')
  # Entitad
  df = add_description(df, cat_entidades, code_col='ENTIDAD', union_code='EDO', description_col='DESCRIP', new_column='NOMBRE_ENTIDAD')
  # MOTEGRE
  df = add_description(df, cat_motivo_egreso, code_col='MOTEGRE', union_code='IdCatMotEgreso', description_col='Descrip', new_column='MOTIVO_EGRESO')
  df = df.drop('MOTEGRE')
  # VEZ
  df = add_description(df, cat_vez, code_col='VEZ', union_code='IDVEZ', description_col='DESCRIP', new_column='VEZ_EGRESO')
  df = df.drop('VEZ')
  # MES_ESTADISTICO
  df = add_description(df, cat_meses, code_col='MES_ESTADISTICO', union_code='IDMES', description_col='MES', new_column='MES_EGRESO')
  df = df.drop('MES_ESTADISTICO')
  
  # 7. Discretize ages
  df = age_discretize(df, age_col='EDAD', new_column='RANGO_EDAD')
  
  # 8. Rename an reoder
  df = df.withColumnRenamed('SEXO_', 'SEXO')
  df = df.select('ID', 'ENTIDAD', 'NOMBRE_ENTIDAD', 'ANIO', 'MES_EGRESO', 'INGRE', 'EGRESO', 'DIAS_ESTA', 'EDAD', 'RANGO_EDAD', 'SEXO', 'NACIO_EN_HOSPITAL', 'MOTIVO_EGRESO', 'VEZ_EGRESO', 'AFECPRIN', 'DESCRIPCION_CAUSA')
  df = df.orderBy('ANIO', 'NOMBRE_ENTIDAD', 'MES_EGRESO')
  
  # 9. Change Sex number to description
  df = df.withColumn('SEXO',
                      F.when(F.col('SEXO') == 'HOMBRE', 'HOMBRES')
                      .when(F.col('SEXO') == 'MUJER', 'MUJERES')
                      .otherwise('DESCONOCIDO'))
  
  # 10. Aggrupation
  df_agg = df.groupBy('ENTIDAD', 'NOMBRE_ENTIDAD', 'ANIO', 'SEXO', 'RANGO_EDAD').agg(F.count('*').alias('TOTAL_EGRESOS'))
  
  # 11. Rename columns
  mapping = {'ENTIDAD': 'ID_ESTADO', 'NOMBRE_ENTIDAD':'ESTADO', 'ANIO': 'ANIO_ANALISIS'}
  df_agg = df_agg.withColumnsRenamed(mapping)
  
  # 12. Save the files
  logging.info("Guardando los datos transformados en formato Parquet...")
  df.write.mode("overwrite").parquet(silver_path_pp)
  df_agg.write.mode("overwrite").parquet(silver_path)
  logging.info(f"¡Éxito! Datos guardados en: {silver_path}")


if __name__=="__main__":
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.join(BASE_PATH, '..', 'data')
    
    # Create silver folder if not exists
    os.makedirs(os.path.join(DATA_PATH, 'silver'), exist_ok=True)
    
    spark_engine = start_spark_engine()
    
    # Dataframe with catalogs
    cat_entidades = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_ENTIDADES.csv'), header=True, inferSchema=True, sep='|')
    cat_cie10 = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_CIE_10_2021.csv'), header=True, inferSchema=True, sep='|')
    cat_tipo_def = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_TIPO_DEF.csv'), header=True, inferSchema=True)
    cat_nacioen = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_SINO.csv'), header=True, inferSchema=True, sep='|')
    cat_sexo = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_SEXO.csv'), header=True, inferSchema=True, sep='|')
    cat_motivo_egreso = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_MOT_EGRESO.csv'), header=True, inferSchema=True, sep='|', encoding='iso-8859-1')
    cat_meses = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_MESES.csv'), header=True, inferSchema=True, sep='|')
    cat_vez = spark_engine.read.csv(os.path.join(DATA_PATH, 'info', 'CAT_VEZ.csv'), header=True, inferSchema=True, sep='|')
    
    try:
        mortality_process(DATA_PATH)
        population_process(DATA_PATH)
        hospital_discharge_process(DATA_PATH)
    except Exception as e:
        logging.error(f'Failure in Silver layer: {e}')
    finally:
        spark_engine.stop()
        logging.info('spark_engine Engine off')