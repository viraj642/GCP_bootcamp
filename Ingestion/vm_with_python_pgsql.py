from google.cloud import bigquery
from google.cloud.sql.connector import Connector
#import pymysql
import sqlalchemy

# Set connection parameters
INSTANCE_NAME = "mypginstance"
DATABASE_NAME = "myorg"
USER = "postgres"
PASSWORD = "A92I58'QhB1=YG^K"
PROJECT_ID = "viraj-patil-bootcamp"
REGION = "us-central1"

# initialize Connector object
connector = Connector()

INSTANCE_CONNECTION_NAME = f"{PROJECT_ID}:{REGION}:{INSTANCE_NAME}"

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=USER,
        password=PASSWORD,
        db=DATABASE_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

connection=pool.connect()

# Connect to BigQuery
bigquery_client = bigquery.Client(project=PROJECT_ID)
database = "pgdataset"

# Table names
TABLE_NAMES = ['employee', 'department', 'project', 'project_staff']

# Loop through all tables
for table_name in TABLE_NAMES:
    # Read table data from Cloud SQL
    result_set = connection.execute(sqlalchemy.text(f"SELECT * FROM {table_name}"))
    rows = result_set.fetchall()

    schema_set = connection.execute(sqlalchemy.text(f"SELECT column_name, data_type FROM {DATABASE_NAME}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table_name}'"))
    table_schema = schema_set.fetchall()

    # Get column names and types from result set metadata
    columns = []
    types = []
    for row in table_schema:
        columns.append(row.column_name)
        if row.data_type == 'character varying':
            types.append('string')
        else:
            types.append(row.data_type)


    # Convert data to list of dictionaries
    data = []
    for row in rows:
        data.append({columns[i]: row[i] for i in range(len(columns))})

    # Create or update table in BigQuery
    table_ref = bigquery_client.dataset(database).table(table_name)
    table = bigquery.Table(table_ref)
    table.schema = [bigquery.SchemaField(columns[i], types[i]) for i in range(len(columns))]
    table = bigquery_client.create_table(table)

    # Get the created table object
    table = bigquery_client.get_table(table_ref)

    # Write data to BigQuery table
    errors = bigquery_client.insert_rows(table, data)

    if errors:
        print(f"Errors inserting data into {table_name}: {errors}")

# Create dept_summary table
query =f"""
    SELECT dept.dept_id,dept.dept_name,COUNT(emp.emp_id) AS emp_count,SUM(emp.salary) AS total_cost,COUNT(DISTINCT(proj_id)) AS proj_count from {database}.employee as emp
    INNER JOIN
    {database}.department as dept
    ON emp.dept_id = dept.dept_id
    INNER JOIN
    {database}.project as prj
    ON prj.dept_id = dept.dept_id
    GROUP BY dept.dept_id,dept.dept_name
"""
job_config = bigquery.QueryJobConfig(destination=f"{PROJECT_ID}.{database}.dept_summary")
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
query_job = bigquery_client.query(query, job_config=job_config)
query_job.result()

# Confirm that the table was created
print(f"Table {database}.dept_summary was created.")

# Create project_staff_count table
query =f"""
    SELECT proj_id, COUNT(DISTINCT emp_id) AS staff_count
    FROM {database}.project_staff
    GROUP BY proj_id
"""
job_config = bigquery.QueryJobConfig(destination=f"{PROJECT_ID}.{database}.project_staff_count")
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
query_job = bigquery_client.query(query, job_config=job_config)
query_job.result()

# Confirm that the table was created
print(f"Table {database}.project_staff_count was created.")