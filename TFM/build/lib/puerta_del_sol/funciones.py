
from pyspark.sql import SparkSession, DataFrame as DF, functions as F
from pyspark.dbutils import DBUtils #type: ignore
from datetime import datetime
import pandas as pd
import re



def anade_dias(spark:SparkSession, df:DF, fecha_inicio:str, fecha_fin:str)->DF:
    df_fechas = spark.sql("""
        SELECT explode(sequence(to_date('{}'), to_date('{}'), interval 1 day)) AS fecha
    """.format(fecha_inicio, fecha_fin))\
        .withColumn('dia_semana', F.dayofweek(F.col('fecha')))\
        .withColumn('mes',F.month(F.col('fecha'))) \
        .withColumn('ano', F.year(F.col('fecha'))) \
    .join(df, on='fecha', how='left')\
        .withColumn('dia_siguiente', F.date_add('fecha', 1))
    df_festivos_siguiente = df.withColumnRenamed('fecha','fecha_siguiente') \
        .withColumnRenamed('tipo', 'tipo_siguiente')
    df_fechas = df_fechas.join(df_festivos_siguiente, on = df_fechas['dia_siguiente'] == df_festivos_siguiente['fecha_siguiente'],\
                               how='left') \
        .withColumn('tipo', F.when(F.col('tipo_siguiente') == 'festivo', 'vispera')\
                    .otherwise(F.col('tipo'))) \
        .withColumn('tipo', F.when(F.col('dia_semana') == 1, 'festivo').\
                    when(F.col('dia_semana') == 7, 'vispera').when(F.col('tipo').isNull(), 'laborable')\
                    .otherwise(F.col('tipo')))\
        .drop('dia_siguiente', 'tipo_siguiente', 'fecha_siguiente') \
        .withColumn('processing_date', F.current_date())
    return df_fechas




def normaliza(col:str,df:DF)->DF:
    df = df.withColumn(col, F.lower(F.col(col)))\
    .withColumn(col,F.regexp_replace(F.col(col), 'á','a')) \
    .withColumn(col,F.regexp_replace(F.col(col),'é','e'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'í', 'i'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'ó', 'o'))\
    .withColumn(col,F.regexp_replace(F.col(col), 'ú', 'u'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'ñ', 'n'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'ä', 'ae'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'ö', 'oe'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'ü', 'ue'))\
    .withColumn(col, F.regexp_replace(F.col(col), 'ß', 'ss'))
    return df



def anade_fecha_ventas(spark:SparkSession, name:str, path:str, dfs:list):
    dbutils = DBUtils(spark)
    dbutils.fs.cp(f'{path}/{name}', 'file:/tmp/tempfile.csv')
    pdf = pd.read_csv('file:/tmp/tempfile.csv', header = None)
    i = 0
    date = re.compile(r'(\d+)\.(\d+)\.(\d+)')
    match = date.search(pdf.iloc[i,1])
    if not match:
        i = i +1
        match = date.search(pdf.iloc[i,1])
        if not match:
            print(f'Nuevo formato en {name}')
            dbutils.fs.rm('file:/tmp/tempfile.csv')
            return
    if match:
        ano, mes, dia = int(match.group(3)), int(match.group(2)),  int(match.group(1))
        fecha = pd.to_datetime(datetime(ano, mes, dia).isoformat())
        try:
            header = pdf.iloc[i+2,1:]
            body = pdf.iloc[i+4:-2,1:]
            body.columns = header.to_list()
            df = spark.createDataFrame(body).withColumn('date', F.lit(fecha)).drop('nan')
            dfs.append(df)
        except:
            raise Exception(f'Nuevo formato de hoja en {name}')
    dbutils.fs.rm('file:/tmp/tempfile.csv')



    # Igual DBUtils(spark) no es necesario y puedo usar directamente dbutils, veremos.