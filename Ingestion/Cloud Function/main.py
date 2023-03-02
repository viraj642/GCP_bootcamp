from google.cloud import bigquery

def gcs_to_bigquery(data, context):
    # Extracting bucket and file name from data
    bucket_name = data['bucket']
    file_name = data['name']
    
    
    # Initializing BigQuery client
    bq_client = bigquery.Client()
    dataset_id = "cfunc"
    table_id = "function_gcs_to_bigquery_data"
    dataset = bq_client.dataset(dataset_id)
    
    # Loading data into BigQuery table
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    uri = "gs://{}/{}".format(bucket_name, file_name)
    job = bq_client.load_table_from_uri(
        uri,
        dataset.table(table_id),
        job_config=job_config
    )

    job.result()
    print(f'File {file_name} loaded into BigQuery table {table_id}.')
