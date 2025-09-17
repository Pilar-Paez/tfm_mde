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

