
from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
from pyspark.dbutils import DBUtils #type: ignore
from datetime import datetime
from functools import reduce
import pandas as pd
import re

from puerta_del_sol.funciones import *


def limpia_tiempo(df:DF)->DF:
    df = df.drop('wdir')\
        .withColumn('prcp',F.when(F.col('prcp').isNull(), F.lit(0)).otherwise(F.col('prcp'))) \
        .withColumn('snow', F.when(F.col('snow').isNull(), F.lit(0)).otherwise(F.col('snow'))) \
        .withColumn('date', F.to_date(F.col('date'))) \
        .withColumn('processing_date', F.current_date())
    ventana = Window.orderBy(F.unix_timestamp(F.col('date')) / 86400).rangeBetween(-3, 3)
    if df.filter(F.col('tsun').isNull()).count() < df.count():  # para evitar while infinito. Pues no lo evita
        while df.filter(F.col('tsun').isNull()).count() > 0:
            df = df.withColumn('tsun', F.when(F.col('tsun').isNull(),\
                        F.round(F.avg('tsun').over(ventana), 1)).otherwise(\
                F.col('tsun'))).cache()
    else:
        df = df.drop('tsun')
    return df




def limpia_festivos(df:DF)->DF:
    df = df.withColumn("fecha", F.to_date(F.col("dia"), "dd-MM-yyyy")).drop('dia')
    cols_ordenadas = ['fecha', 'tipo']
    return df.select(*cols_ordenadas)





def limpia_articulos(df:DF)->DF:
    cols = ['*nummer', 'bezeichnung1', 'preis1', 'preis2', 'preis3', 'ean', 'wgr']
    df = df.dropna(how='all').select(*cols) \
        .withColumn('preis3', F.when(F.col('*nummer') == F.lit(509), 17).otherwise(F.col('preis3'))) \
        .withColumnRenamed('*nummer', 'id') \
        .withColumnRenamed('bezeichnung1', 'name') \
        .withColumn('name', F.when(F.col('id')==1110, F.lit('2 Loeffeln/Gabeln')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==1111, F.lit('4 Loeffeln/Gabeln')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==915, F.lit('Gambas al "pilpil"')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==909, F.lit('Queso mahones frito')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==906, F.lit('Calabacin en crema')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==905, F.lit('Pimientos de Padron')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==904, F.lit('Setas al "pilpil"')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==901, F.lit('Tortilla espanola')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==828, F.lit('Empanada Atun')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==829, F.lit('Emp atun (3,-)')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==830, F.lit('Emp atun (2,80)')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==715, F.lit('Aceitunas con limon')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==711, F.lit('Variacion de Quesos')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==710, F.lit('Datiles asados c/Serrano')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==703, F.lit('Jamon Serrano')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==637, F.lit('Hausgetraenk 3cl')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==441, F.lit('Ramon Bilbao Cr 1/8')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==443, F.lit('Ramon Bilbao EdLim 1/8')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==442, F.lit('Ramon Bilbao Cr 0,7l')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==444, F.lit('Ramon Bilbao EdLim 0,7l')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==345, F.lit('Mia 1/8')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==346, F.lit('Mia 0,7')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==305, F.lit('Cantosan 1/8')).otherwise(F.col('name')))\
        .withColumn('name', F.when(F.col('id')==907, F.lit('espinaca c/pasas y pinones')).otherwise(F.col('name')))\
        .withColumn('name', F.regexp_replace(F.col('name'), '0,7$', '0,7l'))\
        .withColumn('name', F.regexp_replace(F.col('name'), '1/8l$', '1/8'))
    df = normaliza('name',df) \
        .withColumn('name',  F.regexp_replace(F.col('name'), '^d\\.', 'd'))\
        .withColumn('id', F.col('id').cast('string'))\
        .withColumn('processing_date', F.current_date())
    # .withColumn('name', F.lower(F.col('name'))).withColumn('name', F.lower(F.col('name')))\
    return df



def limpia_vinos(df:DF, df_art:DF)->DF:
    df = df.withColumn('Nombre', F.when(F.col('Nombre').isNull(), 'Laureatus')\
                       .otherwise(F.col('Nombre'))) \
        .withColumn('Nombre', F.when(F.col('Tipo').isNull(), None).otherwise(F.col('Nombre')))\
        .dropna(how = 'all') \
        .withColumn('Nombre', F.regexp_replace(F.col('Nombre'), '^d\\.', 'd'))
    df = normaliza('Nombre',df) \
            .withColumn('Nombre', F.rtrim(F.col('Nombre')))
    cols = df.columns[4:]
    df_botellas = df_art.filter(F.col('name').rlike('0,7l$')) \
        .withColumn('name', F.regexp_replace('name', '0,7l', ''))\
        .withColumn('name', F.rtrim(F.col('name'))) \
        .select('id', 'name')
    df_vasos = df_art.filter(F.col('name').rlike('1/8$')) \
        .withColumn('name', F.regexp_replace('name', '1/8', '')) \
        .withColumn('name', F.rtrim(F.col('name'))) \
        .select('id', 'name')
    df = df.join(df_botellas, df.Nombre == df_botellas.name, 'left') \
        .withColumnRenamed('id', 'id_botella').drop('name') \
        .join(df_vasos, df.Nombre == df_vasos.name, 'left') \
        .withColumnRenamed('id', 'id_vaso').drop('name') \
        .withColumn('Uva', F.split(F.lower(F.col('Uva')), ', ')) \
        .withColumn('processing_date', F.current_date())
    for col in cols:
        col_new = col.replace(' ', '_').replace('(', '').replace(')', '')
        df = df.withColumn(col, F.when(F.col(col).isNull(), 0).otherwise(F.col(col)).cast('int')) \
            .withColumnRenamed(col, col_new)  # probar esto que lo he escrito al vuelo
    return df


def limpia_grupos(df:DF)->DF:
    df = df.drop('bezeichnung2','mwst','hgr')\
        .withColumnRenamed('*nummer', 'categoria')\
        .withColumnRenamed('bezeichnung1', 'nombre') \
        .withColumn('nombre', F.when(F.col('categoria') == 3, 'Wein, weiss').otherwise(F.col('nombre')))
    df = normaliza('nombre', df).withColumn('processing_date', F.current_date())
    return df



def limpia_ventas(df:DF, df_art:DF)->DF:
    df = df.withColumnRenamed('Nummer:', 'id').withColumnRenamed('Bezeichnung:', 'name')\
        .withColumn('total_price',F.col('Umsatz:').cast('double'))\
        .withColumn('quantity', F.col('Menge:').cast('int'))\
        .withColumn('date', F.to_date(F.col('date')))\
        .withColumn('unity_price',F.round(F.col('total_price') / F.col('quantity'),1))\
        .withColumn('processing_date', F.current_date())\
        .drop('Anteil%', 'Umsatz:', 'Menge:').filter(F.col('quantity') != 0)
    df = normaliza('name',df) \
        .withColumn('name', F.regexp_replace(F.col('name'), '0,7$', '0,7l'))\
        .withColumn('name', F.regexp_replace(F.col('name'), ' racion$', ''))\
        .withColumn('name',F.regexp_replace(F.col('name'),'^lealtanza','altanza'))\
        .withColumn('name', F.regexp_replace(F.col('name'), ' 10g$', ''))\
        .withColumn('name', F.regexp_replace(F.col('name'), ' 100g$',''))\
        .withColumn('name', F.regexp_replace(F.col('name'),'aceitunas adobo','aceitunas en adobo'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'aceitunas anchoas', 'aceitunas con anchoas'))\
        .withColumn('name',F.regexp_replace(F.col('name'),'aceitunas limon','aceitunas con limon'))\
        .withColumn('name', F.regexp_replace(F.col('name'), '^anchoas', 'filetes de anchoas'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'pepinos chile', 'pepinos en chile'))\
        .withColumn('name', F.regexp_replace(F.col('name'),'postre del dia','crema catalana'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'mahon$', 'queso mahones frito'))\
        .withColumn('name', F.regexp_replace( F.col('name'), 'boquerones$', 'boquerones en vinagre'))\
        .withColumn('name',F.regexp_replace(F.col('name'),'pulpo cilantro','pulpo en cilantro'))\
        .withColumn('name', F.regexp_replace(F.col('name'), '^d\\. ','d '))\
        .withColumn('name', F.regexp_replace(F.col('name'), '^dm ', 'd moreno '))\
        .withColumn('name', F.regexp_replace(F.col('name'),'berenjenas$','berenjenas en escabeche'))\
        .withColumn('name', F.regexp_replace(F.col('name'), ' mitnahme$', ''))\
        .withColumn('name', F.regexp_replace(F.col('name'),'aperol sprizz 0,25','aperol spritzer 0,25l'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'queso iberico', 'manchego'))\
        .withColumn('name',F.regexp_replace(F.col('name'),'alhambra','cerveza spanisch 0,33')) \
        .withColumn('name', F.regexp_replace(F.col('name'), 'd moreno gr reserva', 'd moreno gran reserva')) \
        .withColumn('name', F.regexp_replace(F.col('name'), '0,125', '1/8')) \
        .withColumn('name', F.regexp_replace(F.col('name'), 'cava 0,1$', 'cava 0,1l')) \
        .withColumn('name', F.regexp_replace(F.col('name'), 'cava 0,75$', 'cava 0,7l')) \
        .withColumn('name', F.regexp_replace(F.col('name'), ' gasse', ' 0,7l')) \
        .withColumn('name', F.regexp_replace(F.col('name'), '0,7l 0,7l', '0,7l')) \
        .withColumn('name', F.regexp_replace(F.col('name'), '0,7$', '0,7l')) \
        .withColumn('name', F.regexp_replace(F.col('name'), '1/8l$', '1/8')) \
        .withColumn('name', F.regexp_replace(F.col('name'), 'apfelsaft 1/8', 'apfelsaft 0,125'))
# seguramente esto de cara al bar habría que seguir trabajándolo, pero para el tfm paro aquí
    valores = ['3,-', '2,80', '2,20', '2,50']
    for valor in valores:
        patron = f" \\({valor}\\)"
        df = df.withColumn('name',F.regexp_replace(F.col('name'), patron, ''))
    df = df.withColumn('name', F.regexp_replace(F.col('name'), 'emp ', 'empanada '))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'espin$', 'espinaca'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'ciruela panceta', 'cir&pan'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'cebolla y queso', 'ceb'))\
        .withColumn('name', F.regexp_replace(F.col('name'), 'ceb&ques', 'ceb')) \
        .withColumn('id', F.when(F.col('name') == 'empanada carne', F.lit('801')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada pollo', F.lit('804')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada arabe', F.lit('807')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada humita', F.lit('810')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada caprese', F.lit('813')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada jyq', F.lit('816')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada espinaca', F.lit('819')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada cir&pan', F.lit('822')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada 4 quesos', F.lit('825')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada atun', F.lit('828')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada ceb', F.lit('831')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada picante', F.lit('834')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada arvejas', F.lit('837')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada lentejas', F.lit('840')).otherwise(F.col('id'))) \
        .withColumn('id', F.when(F.col('name') == 'empanada hongos', F.lit('843')).otherwise(F.col('id')))
    window = Window.partitionBy(F.col('name')).orderBy(F.desc(F.col('date')))
    df_new_id = df.withColumn("new_id", F.first(F.col('id')).over(window))
    df_grouped = df_new_id.groupBy(F.col('new_id')).agg(F.collect_set(F.col('name')).alias('repeated_names'))\
        .filter(F.size(F.col('repeated_names')) > 1)
    df_grouped =df_grouped.join(df_art.withColumnRenamed('name','name_art'), \
                                on=df_grouped["new_id"] == df_art["id"], how='left') \
        .withColumn('exploded_names', F.explode(F.col('repeated_names'))) \
        .withColumn('new_id',F.when(F.col('exploded_names') == F.col('name_art'), F.col('new_id'))\
                        .otherwise(F.concat(F.lit("0"), F.col("new_id").cast("string")))) \
        .withColumnRenamed('new_id', 'def_id')
    final_df = df_new_id.join(df_grouped, on = df_new_id['name'] == df_grouped['exploded_names'], how ='left') \
        .withColumn('new_id', F.when(F.col('def_id').isNotNull(), F.col('def_id'))\
                    .otherwise(F.col('new_id'))) \
        .select('new_id', 'name', 'total_price', 'quantity', 'unity_price', 'date')\
        .withColumn('processing_date', F.current_date())
    return final_df



    # Igual DBUtils(spark) no es necesario y puedo usar directamente dbutils, veremos.