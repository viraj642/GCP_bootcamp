import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
from json.decoder import JSONDecodeError


region='us-central1'
runner='DataflowRunner'
project='viraj-patil-bootcamp'
subscription='projects/viraj-patil-bootcamp/subscriptions/dfsub'

# Set project and runner options
options = PipelineOptions(
    streaming=True,
    runner=runner,
    region=region,
    project=project
)

options.view_as(StandardOptions).streaming = True
options.view_as(StandardOptions).runner = 'DataflowRunner'
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'viraj-patil-bootcamp'
google_cloud_options.job_name = 'stream-pubsub-to-bigquery'

# Create pipeline object
pipeline=beam.Pipeline(options=options)

# Read data from Pub/Sub subscription
data = (pipeline| 'Read Data from Pub/Sub' >> beam.io.gcp.pubsub.ReadFromPubSub(subscription=subscription))

# Decode messages and extract the data
lines = (data | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'parse' >> beam.Map(lambda x: {'id': int(x['id']), 'name': x['name']}))

# try:

# except JSONDecodeError as e:
#     print(f"No message found: {e}"

# Write data to BigQuery table
table_spec = bigquery.TableReference(
    projectId='viraj-patil-bootcamp',
    datasetId='dfstream',
    tableId='tstream')

table = (lines | 'ToBigQuery' >> beam.io.WriteToBigQuery(
    table_spec,
    schema='id:INTEGER,name:STRING',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

# Run the pipeline
result = pipeline.run()
result.wait_until_finish()
