# tfm_mde
Repositorio con el código del Trabajo de Fin de Máster del Máster en Big Data & Ingeniería de Datos de María Pilar Páez Guillán.

En este trabajo se pretende ingerir y analizar datos relativos a las consumiciones del restaurante vienés Puerta del Sol. El código escrito, que se puede encontrar en este repositorio, se puede resumir en:

- Canalizaciones y notebooks de ingesta (carpeta azure_synapse)
- Paquete de Python para la lectura y limpieza de datos (carpeta TFM)
- Notebooks de Databricks dedicados a la lectura, limpieza y análisis de los datos, tanto batch como en tiempo real.

### Carpeta azure_synapse

Encontramos los archivos:

- pipeline_varios: código json de la canalización de ingesta de los datos enfocada a ingerir una cantidad elevada de archivos. Está diseñada para copiar y mover archivos desde la ruta unprocessed/input de un Azure Blob Storage.
- pipeline_uno: código json de la canalización de ingesta de los datos enfocada a ingerir un único archivo en cada ejecución. Está diseñada para copiar y mover archivos desde la ruta unprocessed/input/ de un Azure Blob Storage.
- Excel_a_csv_seguro: cuaderno de Python empleado en las canalizaciones para transformar hojas de archivos .xlsx en .csv. Las credenciales están borradas por seguridad.
- Borrado_seguro: cuaderno de Python empleado en las canalizaciones para borrar archivos de las rutas unprocessed/input y unprocessed/auxiliar si han sido copiados con éxito a processed/input y processed/auxiliar, respectivamente


### Carpeta TFM

Esta carpeta contiene el código de un proyecto de PyCharm para crear un paquete de Python destinado a la lectura y limpieza de los datos. El wheel se encuentra en la subcarpeta dist, y el código en sí, en la subcarpeta puerta_del_sol. En esta última encontramos el archivo __init__.py, vacío, y los archivos .py relativos a los tres módulos del paquete:

- funciones.py: con funciones auxiliares que se emplearán en los otros dos archivos.
- lectura.py: funciones para leer cada uno de los tipos de datos con los que trabajaremos en dataframes de Spark.
- limpieza.py: funciones para limpiar cada uno de los dataframes de Spark generados tras leer los datos.


### Carpeta databricks

Encontramos tres subcarpetas.

##### Carpeta batch

Contiene el código referente al flujo de información batch: lectura, limpieza, procesamiento y análisis.

- Bronze_to_silver.ipynb: cuaderno de Databricks dedicado a la lectura de los archivos desde la capa bronze del datalake armado en Azure Data Lake Gen 2, a su limpieza y a la escritura en formato delta en la capa silver de dicho datalake.
- Silver_to_gold.ipynb: cuaderno de Databricks dedicado a la lectura de los archivos desde la capa silver del datalake, a su procesamiento para dejarlos preparados para el análisis, y a su escritura en la capa gold.
- Gold_calculations.ipynb: cuaderno de Databricks dedicado a hacer cálculos en la capa gold, encuadrados en su mayor parte en el análisis exploratorio de datos.
- pipeline_batch: descripción de la canalización que reúne los tres cuadernos anteriores para automatizar su ejecución.
- ML.ipynb: cuaderno de Databricks con los diferentes experimentos de machine learning a los que sometemos los datos gold. Incluye la carga del mejor modelo y una predicción.

##### Carpeta real_time

Contiene el código referente a la simulación de datos en tiempo real y a su lectura, limpieza y análisis.

- Confluent.ipynb: cuaderno de Databricks con la creación de un topic en Confluent y de un stream de Kafka, imitando los pedidos de los clientes durante un día.
- Ingesta_analitica_real_time.ipynb: cuaderno de Databricks dedicado a la lectura del stream simulado, a su escritura en la capa bronze del datalake y a su análisis. Dicho análisis consiste en cotejar cuándo el número de pedidos de cada producto determinado supera un determinado umbral, y enviar mensajes de alerta en un nuevo topic de Kafka creado previamente.
- Bronze_to_silver_real_time.ipynb: cuaderno de Databricks dedicado a transformar la información en tiempo real al mismo formato de la información batch (relativa a las consumiciones diarias) para poder almacenar ambas de modo conjunto.
- pipeline_tiempo_real: descripción de la canalización que reúne los tres cuadernos anteriores para automatizar su ejecución.

##### Carpeta best_model

Contiene el artefacto del mejor modelo obtenido en los experimentos de machine learning (modelo gradient boosting con todas las variables disponibles), necesario para poder replicar el experimento con posterioridad.
