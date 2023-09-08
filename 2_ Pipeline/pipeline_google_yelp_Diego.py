import pandas as pd
import io  # Importa io para StringIO
from google.cloud import storage
from datetime import date
from google.cloud import bigquery
import ast
import datetime
import numpy as np


#-----------------------------------------Funciones Complememntarias--------------------------------------------
#-------Funcion para leer los archivos con la fecha actual de GCS---------------
def archivo_fecha_hoy(carpeta):
    # Reemplaza 'tu-proyecto' y 'tu-bucket' con tu proyecto y nombre de bucket reales.
    project_id = 'finalprojectprototype-397114'
    bucket_name = 'archivos-sin-procesar'

    # Obtén la fecha actual en formato YYYYMMDD
    fecha_hoy = date.today().strftime('%Y%m%d')

    # Crea un cliente de almacenamiento de Google Cloud
    client = storage.Client(project=project_id)

    # Obtiene una referencia al bucket
    bucket = client.bucket(bucket_name)

    # Lista los archivos en la carpeta especificada
    blobs = bucket.list_blobs(prefix=carpeta)

    # Lista para almacenar los DataFrames de archivos encontrados
    dataframes = []

    # Itera sobre los archivos y verifica si contienen la fecha de hoy en el nombre
    for blob in blobs:
        if fecha_hoy in blob.name:
            # Lee el contenido del archivo en un DataFrame
            blob_content = blob.download_as_text()
            df = pd.read_csv(io.StringIO(blob_content))
            dataframes.append(df)

    if not dataframes:
        print(f"No se encontraron archivos con la fecha de hoy en la carpeta {carpeta}.")
        return None

    # Concatena todos los DataFrames en uno solo
    concatenated_df = pd.concat(dataframes, ignore_index=True)

    return concatenated_df

#-------Funcion para leer tablas de big query---------------
def query_bigquery_table(dataset_id, table_id):
    # Crea una instancia del cliente de BigQuery
    client = bigquery.Client(project='finalprojectprototype-397114')

    # Construye la consulta SQL
    query = f"SELECT * FROM `finalprojectprototype-397114.{dataset_id}.{table_id}`"

    # Ejecuta la consulta
    query_job = client.query(query)

    # Convierte los resultados en un DataFrame de pandas
    df = query_job.to_dataframe()

    return df

#-----------------------------------------Funciones para ETL'S--------------------------------------------
#-------ETL google review---------------
def etl_google_review(df):
    if df is None:
        return 


    for i in range(0, len(df)):
        df['time'][i] = datetime.datetime.fromtimestamp(df['time'][i]/1000.0)
    df['pics'] = np.where(df['pics'].isnull(), 'No', 'Si')

    def extract_text(data):
        if isinstance(data, dict) and 'text' in data:
            return data['text']
        return None

    def extract_text2(data):
        if isinstance(data, dict) and 'time' in data:
            return data['time']
        return None

    # Aplica la función a la columna 'data' y crea una nueva columna 'text'
    df['resp_text'] = df['resp'].apply(extract_text)
    df['resp_time'] = df['resp'].apply(extract_text2)

    pd.set_option('display.float_format', '{:.0f}'.format)
    for i in range(0, len(df)):
        if pd.notna(df['resp_time'][i]):  # Verificar si el valor no es nulo
            df['resp_time'][i] = datetime.datetime.fromtimestamp(df['resp_time'][i]/1000.0)

    columnas_a_eliminar = ['resp_text', 'resp_time']
    df = df.drop(columnas_a_eliminar, axis=1)
    df = df.drop(['resp'], axis=1)
    df["time"] = pd.to_datetime(df["time"])

    # Dropeo filas duplicadas
    df = df.drop_duplicates()

    # Cliente bigquery
    client = bigquery.Client(project='finalprojectprototype-397114')

    try:
        df.to_gbq(destination_table='finalprojectprototype-397114.googlemaps.fact_gm_review', project_id='finalprojectprototype-397114', if_exists='fail')
    except Exception as e:
        print("La tabla fact_gm_review ya existe. Se insertarán los nuevos datos. Detalles del error:", str(e))
        df.to_gbq(destination_table='finalprojectprototype-397114.googlemaps.fact_gm_review', project_id='finalprojectprototype-397114', if_exists='append')

#-------ETL google business---------------

def etl_google_business(df_metadata_input):

    if df_metadata_input is None:
        return 

    def extract_key_values(misc_dict, key):
        if isinstance(misc_dict, dict) and key in misc_dict:
            return ', '.join(misc_dict[key])
        else:
            return np.nan

    def format_category_list(category_list):
        if isinstance(category_list, list):
            return ', '.join(category_list)
        else:
            return category_list

    # Eliminamos columnas y duplicados de filas
    df_metadata_input = df_metadata_input.drop(columns=['relative_results', 'url'])
    df_metadata_input = df_metadata_input.drop_duplicates()

    # -------------1. FILTRO FLORIDA --------------------------------------
    # Definir los rangos de latitud y longitud que corresponden a Florida
    min_lat = 24.396308
    max_lat = 31.000000
    min_lon = -87.634792
    max_lon = -79.974307

    # Filtrar el DataFrame para incluir solo ubicaciones dentro de los rangos de latitud y longitud de Florida
    df_metadata_input = df_metadata_input[(df_metadata_input['latitude'] >= min_lat) &
                                          (df_metadata_input['latitude'] <= max_lat) &
                                          (df_metadata_input['longitude'] >= min_lon) &
                                          (df_metadata_input['longitude'] <= max_lon)]

    # -----------------2. Filtramos categoria Restaurant ------------------------------------
    df_metadata_input['category'] = df_metadata_input['category'].apply(lambda x: ast.literal_eval(x)
                                                                        if isinstance(x, str) else np.nan)
    df_metadata_input['category'] = df_metadata_input['category'].apply(format_category_list)
    # Filtramos categoria Restaurant
    business_list = ['Restaurant']
    df_metadata_input = df_metadata_input[df_metadata_input['category'].isin(business_list)]

    # -----------------3. DESANIDAR MISC -----------------------------------------------------
    # Parse the string representation of the dictionary
    df_metadata_input['MISC'] = df_metadata_input['MISC'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else np.nan)
    desired_keys = ['Service options', 'Highlights', 'Popular for', 'Accessibility', 'Offerings', 'Dining options',
                    'Crowd', 'Atmosphere', 'Payments', 'Amenities', 'Service options', 'Planning']
    # Create columns for each key
    for key in desired_keys:
        df_metadata_input[key] = df_metadata_input['MISC'].apply(lambda x: extract_key_values(x, key))
    # Drop the 'MISC' column if no longer needed
    df_metadata_input = df_metadata_input.drop(columns=['MISC'])
    # Se eliminan las columnas planning, payments, highlights
    df_metadata_input.drop(columns=['Planning', 'Payments', 'Highlights'], inplace=True)

    # ----------------- 4. Desanidar HOURS -----------------------------------------------------
    # De <class 'str'> a <class 'list'>
    df_metadata_input['hours'] = df_metadata_input['hours'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    # De <class 'list'> a <class 'dict'>
    df_metadata_input['hours'] = df_metadata_input['hours'].apply(lambda x: dict(x) if isinstance(x, list) else x)
    # De <class 'dict'> a columnas
    hours_df = df_metadata_input['hours'].apply(pd.Series)
    # Concatenamos las columnas
    df_metadata_input = pd.concat([df_metadata_input, hours_df], axis=1)
    df_metadata_input = df_metadata_input.drop(columns=['hours', 0])
    # LLenamos campos nulos "sin datos"
    df_metadata_input.fillna('sin datos')

    # Initialize the BigQuery client
    client = bigquery.Client(project='finalprojectprototype-397114')

    try:
        df_metadata_input.to_gbq(destination_table='finalprojectprototype-397114.googlemaps.dim_gm_business', project_id='finalprojectprototype-397114', if_exists='fail')
    except Exception as e:
        print("La tabla dim_gm_business ya existe. Se insertarán los nuevos datos. Detalles del error:", str(e))
        df_metadata_input.to_gbq(destination_table='finalprojectprototype-397114.googlemaps.dim_gm_business', project_id='finalprojectprototype-397114', if_exists='append')

#-------ETL yelp business---------------
def etl_yelp_business(df):

    if df is None:
        return 

    columnas = [
        'business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude',
        'stars', 'review_count', 'is_open', 'attributes', 'hours']

    # Eliminar las filas con valores nulos en la columna "categories"
    df = df.dropna(subset=['categories'])

    # Filtrar el DataFrame para incluir solo filas que contengan "Restaurants" en la columna "categories"
    df = df[df['categories'].str.contains('Restaurants', case=False, na=False)]

    # Eliminar columnas que terminan con '.1'
    df = df.loc[:, ~df.columns.str.endswith('.1')]

    # Definir los rangos de latitud y longitud que corresponden a Florida
    min_lat = 24.396308
    max_lat = 31.000000
    min_lon = -87.634792
    max_lon = -79.974307

    # Filtrar el DataFrame para incluir solo ubicaciones dentro de los rangos de latitud y longitud de Florida
    df = df[(df['latitude'] >= min_lat) &
            (df['latitude'] <= max_lat) &
            (df['longitude'] >= min_lon) &
            (df['longitude'] <= max_lon)]

    # Establecer todos los valores en la columna "state" como "FL"
    df['state'] = 'FL'

    # Dividir la columna 'categories' en una lista de categorías
    df['categories'] = df['categories'].str.split(', ')

    # Crear un conjunto de todas las categorías únicas presentes en el DataFrame
    unique_categories = set(category for categories_list in df['categories'] for category in categories_list)

    # Crear columnas de dummies para cada categoría y establecer valores True/False
    for category in unique_categories:
        df[category] = df['categories'].apply(lambda x: category in x)

    # Eliminar la columna original de 'categories'
    df = df.drop(columns=['categories'])

    # Llenar NaN con False en las columnas de categorías
    df = df.fillna(False)

    # Obtener las 10 categorías más comunes
    top_categories = df[list(unique_categories)].sum().nlargest(10).index

    # Filtrar el DataFrame por las top 10 categorías
    df = df[columnas + list(top_categories)]

    df['attributes'] = df['attributes'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})
    df['hours'] = df['hours'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})

    # ----------------- 4. Desanidar Atributos -----------------------------------------------------

    desired_keys = ['RestaurantsPriceRange2', 'BusinessAcceptsCreditCards', 'RestaurantsReservations', 'Ambience', 'GoodForKids', 'RestaurantsDelivery', 'OutdoorSeating', 'Alcohol', 'RestaurantsGoodForGroups']

    # Define the function to format the attributes dictionary
    def format_attributes_dict(attributes_dict):
        if isinstance(attributes_dict, dict):
            filtered_dict = {k: attributes_dict[k] for k in desired_keys if k in attributes_dict}
            return filtered_dict
        else:
            return attributes_dict

    # Apply the function to the 'attributes' column in your DataFrame 'df'
    df['attributes'] = df['attributes'].apply(format_attributes_dict)

    # Create new columns from the filtered attributes dictionary
    df = pd.concat([df.drop('attributes', axis=1), df['attributes'].apply(pd.Series)], axis=1)

    desired_keys = ['touristy', 'hipster', 'romantic', 'divey', 'intimate', 'trendy', 'upscale', 'classy', 'casual']

    df['Ambience'] = df['Ambience'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})

    for key in desired_keys:
        df[key] = df['Ambience'].apply(lambda x: x.get(key, None) if isinstance(x, dict) else None)

    df.drop(columns=['Ambience'], inplace=True)

    df.rename(columns={'American (Traditional)': 'American_Traditional'}, inplace=True)
    df.rename(columns={'American (New)': 'American_New'}, inplace=True)
    df = df.reset_index(drop=True)

    # ----------------- 4. Desanidar hours -----------------------------------------------------

    flattened_datah = pd.json_normalize(df['hours'])
    df = pd.concat([df, flattened_datah], axis=1)
    df = df.astype(str)
    df['review_count'] = df['review_count'].astype(int)
    df = df.drop('hours', axis=1)

    try:
        df.to_gbq(destination_table='finalprojectprototype-397114.yelp.dim_yelp_business', project_id='finalprojectprototype-397114', if_exists='fail')
    except Exception as e:
        print("La tabla dim_yelp_business ya existe. Se insertarán los nuevos datos. Detalles del error:", str(e))
        df.to_gbq(destination_table='finalprojectprototype-397114.yelp.dim_yelp_business', project_id='finalprojectprototype-397114', if_exists='append')

#-------ETL yelp checkin---------------
def etl_yelp_checkin(df_checkin, df_business):
    if df_checkin is None:
        return 

    # Realizar la operación de merge en los DataFrames
    df = pd.merge(df_checkin, df_business, on='business_id')
    df = df[['business_id', 'date']]
    df['checkin_count'] = df['date'].apply(lambda x: len(x.split(', ')))
    df = df[['business_id', 'checkin_count']]

    try:
        df.to_gbq(destination_table='finalprojectprototype-397114.yelp.fact_yelp_checkin', project_id='finalprojectprototype-397114', if_exists='fail')
    except Exception as e:
        print("La tabla yelp.fact_yelp_checkin ya existe. Se insertarán los nuevos datos. Detalles del error:", str(e))
        df.to_gbq(destination_table='finalprojectprototype-397114.yelp.fact_yelp_checkin', project_id='finalprojectprototype-397114', if_exists='append')

#-------ETL yelp review---------------
def etl_yelp_review(df_review, df_business):
    if df_review is None:
        return 

    # Realizar la operación de merge en los DataFrames
    df = pd.merge(df_review, df_business, on='business_id')
    df = df[["review_id",	"user_id",	"business_id",	"stars_x",	"useful",	"funny",	"cool",	"text",	"date"]]
    df["date"] = pd.to_datetime(df["date"])

    try:
        df.to_gbq(destination_table='finalprojectprototype-397114.yelp.fact_yelp_review', project_id='finalprojectprototype-397114', if_exists='fail')
    except Exception as e:
        print("La tabla fact_yelp_review ya existe. Se insertarán los nuevos datos. Detalles del error:", str(e))
        df.to_gbq(destination_table='finalprojectprototype-397114.yelp.fact_yelp_review', project_id='finalprojectprototype-397114', if_exists='append')


etl_google_review(archivo_fecha_hoy('google/Google_review'))
etl_google_business(archivo_fecha_hoy('google/Google_business'))

etl_yelp_business(archivo_fecha_hoy('yelp/Yelp_business'))
etl_yelp_checkin(archivo_fecha_hoy('yelp/Yelp_checkin'), query_bigquery_table('yelp', 'dim_yelp_business'))
etl_yelp_review(archivo_fecha_hoy('yelp/Yelp_review'), query_bigquery_table('yelp', 'dim_yelp_business'))
