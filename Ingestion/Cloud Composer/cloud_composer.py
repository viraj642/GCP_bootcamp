from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago


default_args = {
    'start_date': days_ago(1),
    'retries': 0
}

dag = DAG(
    'upload_avro_to_bigquery',
    default_args=default_args,
    description='Upload Avro file data from GCS to BigQuery table'
)

# Set the GCS bucket and file names
gcs_bucket = "viraj-patil-composer"
gcs_file_path = "data.avro"

# Set the BigQuery dataset and table names
bigquery_dataset = "dag_dataset"
bigquery_table = "composer_data"


# Define the GCS to BigQuery operator
gcs_to_bq = GCSToBigQueryOperator(
    task_id='upload_to_bq',
    bucket=gcs_bucket,
    source_objects=[gcs_file_path],
    destination_project_dataset_table=f'{bigquery_dataset}.{bigquery_table}',
    source_format='AVRO',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    encoding='ISO-8859-1',
    allow_jagged_rows=True,
    ignore_unknown_values=True,
    dag=dag
)
