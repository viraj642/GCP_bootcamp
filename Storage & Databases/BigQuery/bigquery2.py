from google.cloud import bigquery
from google.oauth2 import service_account
import sys

# Set key_path to the path to the service account keyfile.
if len(sys.argv)==2:
    key_path=sys.argv[1]
else:
    key_path = "keys/bigquerySA_key.json"

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

credentials = authenticateServiceAccount(key_path)

def BigQueryOperations(credentials):
    if not credentials:
        return

    try:
        # Create a BigQuery Client using the credentials.
        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

        # Define the dataset and table references
        dataset_name = 'bikeshare'
        table_name = 'hourly_summary_trips'
        #view_name = 'busiest_stations_by_hour'

        # Create the dataset
        dataset = bigquery.Dataset(client.dataset(dataset_name))
        dataset.location = 'US'
        dataset.default_table_expiration_ms = 30 * 24 * 60 * 60 * 1000  # 30 days
        dataset = client.create_dataset(dataset)

        # Define the SQL query to generate the hourly summary
        query = f"""
        SELECT
        DATE(start_time) AS trip_date,
        EXTRACT(HOUR FROM start_time AT TIME ZONE "UTC") AS trip_start_hour,
        start_station_name,
        COUNT(*) AS trip_count,
        SUM(duration_minutes) AS total_trip_duration_minutes
        FROM
        `bigquery-public-data.austin_bikeshare.bikeshare_trips`
        GROUP BY
        trip_date, trip_start_hour, start_station_name
        """

        # Create the table
        table = bigquery.Table(client.dataset(dataset_name).table(table_name))
        table.partitioning_type = 'DAY'
        table.clustering_fields = ['start_station_name']

        # Write the query results to the table
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        query_job = client.query(query, job_config=job_config)
        query_job.result()

        # Confirm that the table was created
        print(f"Table {dataset_name}.{table_name} was created.")

        # # Define the SQL statement to create the view
        # create_view_query = f"""
        # CREATE VIEW {dataset_name}.{view_name} AS
        # SELECT
        #     trip_date,
        #     trip_start_hour,
        #     start_station_name AS station_name,
        #     MAX(trip_count) AS max_trips
        # FROM
        #     {dataset_name}.{table_name}
        # GROUP BY
        #     trip_date,
        #     trip_start_hour,
        #     station_name
        # """

        # # Execute the SQL statement to create the view
        # client.query(create_view_query).result()


        # # Confirm that the view was created
        # print(f"View {dataset_name}.{view_name} was created.")

    except Exception as e:
        print(f"Error in BigQuery operations: {e}")

BigQueryOperations(credentials)
