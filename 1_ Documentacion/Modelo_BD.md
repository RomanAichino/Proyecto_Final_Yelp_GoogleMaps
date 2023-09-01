## MODELO DE DATOS
Se dispone como fuente de información los archivos de las dos plataformas, compuestos por archivos de tipo json, parquet, pkl.

Para poder tener un mejor entendimiento de las relaciones básicas de estas plataformas construimos el modelo de base de datos inicial, nos sirve de referencia para el proceso de ETL.

El modelo final nos sirve para ordenar la extracción de información correspondiente al estado de FLORIDA que figura en el alcance del proyecto, según las dependencias que figuran en el modelo. 

Asimismo dentro del proceso de ETL generamos un modelo de datos para el datawarehouse, eliminando columnas, transformándolas, calculando nuevas columnas, etc. Las relaciones de las tablas de este modelo son las que rigen para la tarea de carga de datos al datawarehouse.

TABLAS GOOGLE MAPS
![Google_1]("8_Imagenes/modelo_google_1.png)

