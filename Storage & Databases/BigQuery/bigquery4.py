from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import sys


# Set key_path to the path of the google storage service account keyfile.
if len(sys.argv) == 3:
    storageKey_path = sys.argv[1]
    bigqueryKey_path = sys.argv[2]
else:    
    storageKey_path = "keys/gsa_keyfile.json"
    bigqueryKey_path = "keys/bigquerySA_key.json"

# To authenticate with a service account key file create credentials.
def authenticateServiceAccount(key_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials
    except Exception as e:
        print(f"Failed to authenticate with service account key file: {e}")
        return None

try:
    credentials = authenticateServiceAccount(storageKey_path) 
    bucket_name = "viraj-patil-netflix"
    file_name = "netflix-rotten-tomatoes-metacritic-imdb.csv"

    def storageOperations(credentials):

        if credentials==None:
            return
        
        # Create google storage client
        gs_client = storage.Client(credentials=credentials)

        location = "us-central1"
        storage_class = "standard"

        # Create storage bucket 
        bucket = gs_client.create_bucket(bucket_name,location=location)

        # Verify bucket is created successfully
        print("Bucket {} created".format(bucket.name))

        # Create a blob object that represents the file
        blob = bucket.blob(file_name)

        # Upload the file to the bucket
        local_file_path = "./netflix-rotten-tomatoes-metacritic-imdb.csv"

        blob.upload_from_filename(local_file_path)

    storageOperations(credentials)
except Exception as e:
    print(f"Error occurred while performing storage operations: {e}")
    sys.exit(1)

# To authenticate with a bigquery service account
try:
    credentials = authenticateServiceAccount(bigqueryKey_path)

    def bigqueryOperations(credentials):

        if credentials==None:
            return
        
        # Create a BigQuery Client using the credentials.
        bq_client = bigquery.Client(credentials=credentials,project=credentials.project_id)

        # Define the dataset and table references
        dataset_name = 'netflix'
        table_name = 'netflix-raw-data'

        # Create the dataset
        dataset = bigquery.Dataset(bq_client.dataset(dataset_name))
        dataset.location = 'US'
        dataset.default_table_expiration_ms = 30 * 24 * 60 * 60 * 1000  # 30 days
        dataset = bq_client.create_dataset(dataset)

        # Load data from Google Cloud Storage to BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.CSV

        uri = "gs://{}/{}".format(bucket_name, file_name)
        load_job = bq_client.load_table_from_uri(
            uri, dataset.table(table_name), job_config=job_config
        )

        load_job.result()

    bigqueryOperations(credentials)
except Exception as e:
    print(f"Error occurred while performing BigQuery operations: {e}")
    sys.exit(1)