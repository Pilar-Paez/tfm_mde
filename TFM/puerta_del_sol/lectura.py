
from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
from pyspark.dbutils import DBUtils #type: ignore
from datetime import datetime
from functools import reduce
import pandas as pd
import re

from puerta_del_sol.funciones import *

def lee_tiempo(spark:SparkSession, path:str, path_tiempo:str)->DF:
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(path)
    names = [file.name for file in files]
    for name in names:
        match = re.search(r'tiempo_(\d+).csv', name)
        if match:
            dbutils.fs.mv(f'{path}{name}', f'{path_tiempo}{name}')
    return spark.read.option('header', True).option('inferSchema', True) \
        .option('delimiter', ',').csv(f'{path_tiempo}*.csv')


def lee_festivos(spark:SparkSession, path:str)->DF:
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(path)
    names = [file.name for file in files]
    for name in names:
        if name.endswith('.json'):
            df = spark.read.format('json').option('multiLine', True).load(f'{path}{name}')
            #dbutils.fs.mv(f'{path}{name}', f'{path_festivos}{name}') # esto sería para no volver a leerlo.
            # debería hacer algo similar con todos, o bueno, ya veré cómo lo gestiono. Bueno, si luego llegan
            # de uno en uno, igual no es muy trambólico
            return df
        else:
            df = None
    return df



def lee_articulos(spark:SparkSession, path:str)->DF:
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(path)
    names = [file.name for file in files]
    for name in names:
        if name.startswith('PdS Artikel'):
            df =spark.read.option('header', 'true').option('inferSchema', 'true').option('delimiter', ';').csv(
                f'{path}{name}')
            #dbutils.fs.mv(f'{path}{name}', f'{path_articulos}{name}') # esto sería para no volver a leerlo.
            return df
        else:
            df = None
    return df



def lee_vinos(spark:SparkSession, path:str)->DF:
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(path)
    names = [file.name for file in files]
    for name in names:
        if name.startswith('vinos'):
            df =spark.read.option('header', 'true').option('inferSchema', 'true')\
                .option('delimiter', ',').csv(f'{path}{name}')
            #dbutils.fs.mv(f'{path}{name}', f'{path_vinos}{name}') # esto sería para no volver a leerlo.
            return df
        else:
            df = None
    return df



def lee_grupos(spark:SparkSession, path:str)->DF:
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(path)
    names = [file.name for file in files]
    for name in names:
        if name.startswith('PdS Warengruppen'):
            df =spark.read.option('header', 'true').option('inferSchema', 'true')\
                .option('delimiter', ';').csv(f'{path}{name}')
            #dbutils.fs.mv(f'{path}{name}', f'{path_grupos}{name}') # esto sería para no volver a leerlo.
            return df
        else:
            df = None
    return df


def lee_ventas(spark:SparkSession, path:str)->DF:
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(path)
    names = [file.name for file in files]
    dfs = []
    for name in names:
        match = re.search(r'PdS Daten (\d+).xlsx_Tabellenblatt(\d+).csv', name)
        if match:
            anade_fecha_ventas(spark,name,path,dfs)
            #dbutils.fs.mv(f'{path}{name}', f'{path_ventas}{name}')
    df = reduce(lambda a, b: a.unionByName(b), dfs) if dfs else None
    return df

    # Igual DBUtils(spark) no es necesario y puedo usar directamente dbutils, veremos.