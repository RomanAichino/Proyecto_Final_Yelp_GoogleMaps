import os
import base64
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
from google.api_core.exceptions import NotFound

# Where the files come from, in GCS?
project_files_id = 'finalprojectprototype-397114'
gcs_bucket_name = 'api_files_tragon'
gcs_prefix = 'airquality' # prefix to filter the files we want to load incrementally

# Where we want to load the in BigQuery?
bigquery_dataset_id = 'increment_load'
bigquery_table_id = 'loaded_files'  

# Where do we want store loaded file names? (Log file)
loaded_files_dataset_id = 'increment_load'
loaded_files_table_id = 'log_file'  

storage_client = storage.Client()
bigquery_client = bigquery.Client()

def get_loaded_files():
    table_ref = bigquery_client.dataset(loaded_files_dataset_id, project=project_files_id).table(loaded_files_table_id)
    
    try:
        table = bigquery_client.get_table(table_ref)    
    except NotFound:
        # Handle the case where the table doesn't exist yet
        print(f"Loaded files table does not exist. Creating it...")
        schema = [
            bigquery.SchemaField("file_name", "STRING")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
    else:
        schema = table.schema

    query_job = bigquery_client.query(f"SELECT file_name FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`")
    results = query_job.result()
    return [row.file_name for row in results]

def update_loaded_files(file_names):
    existing_file_names = get_loaded_files()
    try:
        new_file_names = [file_name for file_name in file_names if file_name not in existing_file_names]
        
        if new_file_names:
            table_ref = bigquery_client.dataset(loaded_files_dataset_id).table(loaded_files_table_id)
            table = bigquery_client.get_table(table_ref)
            rows_to_insert = [{'file_name': file_name} for file_name in new_file_names]
            bigquery_client.insert_rows(table, rows_to_insert)
    except Exception as e:
        print(f"An error occurred while updating loaded files table: {e}")

def load_csv_files_to_bigquery(bucket_name, prefix, dataset_id, table_id):
    
    # Get the list of loaded file names from BigQuery
    loaded_file_names = get_loaded_files()

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    
    try:
        table = bigquery_client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_id} does not exist. Creating it...")
        schema = [
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("data_city", "STRING"),
            bigquery.SchemaField("data_state", "STRING"),
            bigquery.SchemaField("data_country", "STRING"),
            bigquery.SchemaField("data_location_type", "STRING"),
            bigquery.SchemaField("data_location_coordinates", "STRING"),
            bigquery.SchemaField("data_current_pollution_ts", "TIMESTAMP"),
            bigquery.SchemaField("data_current_pollution_aqius", "INTEGER")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
    else:
        schema = table.schema

    # Get the list of loaded file names from BigQuery
    #loaded_file_names = get_loaded_files()

    blobs = storage_client.list_blobs(bucket_name, prefix=gcs_prefix)
    for blob in blobs:
        file_name = blob.name

        if not file_name.endswith('.csv'):
            print(f"Skipping file {file_name} as it's not a CSV file.")
            continue
        
        if file_name not in loaded_file_names:
            file_uri = f"gs://{bucket_name}/{file_name}"
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip the header row if present
                allow_quoted_newlines=True,
                allow_jagged_rows=True,
                field_delimiter=',',
                autodetect=True,  # Automatically detect schema from data
            )

            # Load data from the CSV file into BigQuery
            uri = [file_uri]
            load_job = bigquery_client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config,
            )

            load_job.result()  # loading job, just wait...
            print(f"Loaded file {file_name} into BigQuery.")
            loaded_file_names.append(file_name)

        # Update the loaded files table in BigQuery with the updated list
        update_loaded_files(loaded_file_names)

def process_pubsub(event, context):
    try:
        if 'data' in event:
            data_str = base64.b64decode(event['data']).decode('utf-8')
            if data_str:
                print(f"Received data: {data_str}")
                load_csv_files_to_bigquery(gcs_bucket_name, gcs_prefix, bigquery_dataset_id, bigquery_table_id)
            else:
                print("Received empty data string.")
        else:
            print("No 'data' field found in the Pub/Sub event.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Trigger the function manually for testing in local terminal
if __name__ == "__main__":
    sample_event = {
        'data': base64.b64encode(b'Sample data').decode('utf-8')
    }
    process_pubsub(sample_event, None)
