import datetime
import requests
import pandas as pd
from google.cloud import storage

# La API permite la solicitud de datos para una sola ciudad a la vez, y hasta un maximo de 5 ciudades una despues de otra.

def fetch_air_quality_data():
    cities = ["Tampa", "Clearwater", "Saint Petersburg", "Largo", 'Miami']
    api_key = "62bf8dcc-4a80-49d2-9a04-cf7e30236634"
    dataframes = []

    for city in cities:
        url = f"http://api.airvisual.com/v2/city?city={city}&state=Florida&country=USA&key={api_key}"
        response = requests.get(url)
        data = response.json()
        airquality = pd.json_normalize(data)
        dataframes.append(airquality)

    airquality = pd.concat(dataframes, axis=0)
    airquality.reset_index(drop=True, inplace=True)
    airquality = airquality[['status', 'data.city', 'data.state', 'data.country', 'data.location.type',
                             'data.location.coordinates', 'data.current.pollution.ts', 'data.current.pollution.aqius']]

    current_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    destination_path = f'airquality_{current_date}.csv'

    storage_client = storage.Client()
    bucket_name = 'api_files_tragon'  
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_path)

    csv_str = airquality.to_csv(index=False)
    blob.upload_from_string(csv_str)

    return f"Air quality data saved to {destination_path}" 

def main(event, context):
    fetch_air_quality_data()

if __name__ == "__main__":
    main('data', 'context')