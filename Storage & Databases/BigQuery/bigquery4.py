from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account


# Set key_path to the path of the google storage service account keyfile.
key_path = "keys/gsa_keyfile.json"

# To authenticate with a service account key file create credentials. 
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Create google storage client
gs_client = storage.Client(credentials=credentials)

bucket_name = "viraj-patil-netflix"
location = "us-central1"
storage_class = "standard"

# Create storage bucket 
bucket = gs_client.create_bucket(bucket_name,location=location)

# Verify bucket is created successfully
print("Bucket {} created".format(bucket.name))

# Create a blob object that represents the file
file_name = "netflix-rotten-tomatoes-metacritic-imdb.csv"

blob = bucket.blob(file_name)

# Upload the file to the bucket
local_file_path = "./netflix-rotten-tomatoes-metacritic-imdb.csv"

blob.upload_from_filename(local_file_path)


# Set key_path to the path of the BigQuery service account keyfile.
key_path = "keys/bigquerySA_key.json"

# To authenticate with a service account key file create credentials. 
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Get the bucket IAM policy
#policy = bucket.get_iam_policy(requested_policy_version=3)

# Add a new binding to the policy with the storage.objectViewer role and the specified service account
#policy.bindings.append({
#    "role": "roles/storage.objectViewer",
#    "members": {"serviceAccount":"serviceAccount:bqadminsa@viraj-patil-bootcamp.iam.gserviceaccount.com"},
#})

# Update the bucket IAM policy
#bucket.set_iam_policy(policy)

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

# # Write query to create a view of title count in each country availability group by run time 
# view_name="view_titles_by_country_runtime"
# sql_query = f"""
# CREATE VIEW {dataset_name}.{view_name} AS
# SELECT
#   Country Availability,
#   Runtime,
#   COUNT(Title) AS Titles_Count
# FROM
#   {dataset_name}.{table_name}
# GROUP BY
#   Country Availability, Runtime
# """

# # Execute the SQL statement to create the view
# bq_client.query(sql_query).result()


# # Confirm that the view was created
# print(f"View {dataset_name}.{view_name} was created.")

# # Write a query to create view of no. titles for each actor.
# view_name="view_titles_by_actor"
# sql_query = f"""
# WITH actors AS (
#   SELECT Actor, Title
#   FROM {dataset_name}.{table_name}, UNNEST(SPLIT(Actors, ',')) AS Actor
# )
# CREATE VIEW {dataset_name}.{view_name} AS
# SELECT Actor, COUNT(Title) AS title_count
# FROM actors
# GROUP BY Actor
# """

# # Execute the SQL statement to create the view
# bq_client.query(sql_query).result()


# # Confirm that the view was created
# print(f"View {dataset_name}.{view_name} was created.")

# # Write a query to create view of no. titles for each genre
# view_name="view_titles_by_genre"
# sql_query = f"""
# WITH genres AS (
#   SELECT Genre, Title
#   FROM {dataset_name}.{table_name}, UNNEST(SPLIT(Genre, ',')) AS Genre
# )
# CREATE VIEW {dataset_name}.{view_name} AS
# SELECT Genre, COUNT(Title) AS title_count
# FROM genres
# GROUP BY Genre
# """

# # Execute the SQL statement to create the view
# bq_client.query(sql_query).result()


# # Confirm that the view was created
# print(f"View {dataset_name}.{view_name} was created.")

# # create View : Find out the number of Titles available in each Country Availability by Genre.
# view_name="view_titles_by_country_gener"
# sql_query = f"""
# WITH genres AS (
#   SELECT Genre, Title, Country Availability
#   FROM {dataset_name}.{table_name}, UNNEST(SPLIT(Genre, ',')) AS Genre
# )
# CREATE VIEW {dataset_name}.{view_name} AS
# SELECT Country Availability, Genre, COUNT(Title) AS title_count
# FROM geners
# GROUP BY Country Availability,Genre
# """

# # Execute the SQL statement to create the view
# bq_client.query(sql_query).result()


# # Confirm that the view was created
# print(f"View {dataset_name}.{view_name} was created.")