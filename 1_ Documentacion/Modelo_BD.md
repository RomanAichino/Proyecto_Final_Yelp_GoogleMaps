## MODELO DE DATOS
Se dispone como fuente de información los archivos de las dos plataformas, compuestos por archivos de tipo json, parquet, pkl.

Para poder tener un mejor entendimiento de las relaciones básicas de estas plataformas construimos el modelo de base de datos inicial, nos sirve de referencia para el proceso de ETL.

El modelo inicial nos sirve para ordenar la extracción de información correspondiente al estado de FLORIDA que figura en el alcance del proyecto, según las dependencias que figuran en el modelo. 

Asimismo dentro del proceso de ETL generamos un modelo de datos para el datawarehouse, eliminando columnas, transformándolas, calculando nuevas columnas, etc. Las relaciones de las tablas de este modelo final son las que rigen para la tarea de carga de datos al datawarehouse.

TABLAS GOOGLE MAPS 

<img src="https://1000logos.net/wp-content/uploads/2021/05/Google-Maps-logo.png"  height=50>

<img src=".\modelo_google_1.png"  height=300> 

TABLAS YELP

<img src="https://1000logos.net/wp-content/uploads/2018/03/Yelp-Logo.png"  height=50>

<img src=".\modelo_yelp_1.png"  height=300> 

## Modelo de Datawarehouse

Estas estructuras se van a crear en BIGQUERY para hacer la carga.

Tablas GOOGLE MAPS

<img src=".\modelo_google_2.png"  height=300> 

* Las columnas category y MISC se han desanidado en múltiples columnas, que se presentaran en el diccionario de datos.

## Tablas YELP

Se muestran en mayúsculas las columnas modificadas o agregadas al modelo. Por ejemplo las columnas User.FRIENDS_COUNT y User.ELITE_COUNT.

<img src=".\modelo_yelp_2.png"  height=300> 


