# Save BigQuery results to Cloud Storage

# Libraries ... 
from google.cloud import bigquery


def query_to_storage(PROJECT_ID, DATASET, TABLE, LOCATION, SQL_PATH, SQL_PATH, BUCKET_URI):
    QUERY = open(SQL_PATH).read()

    # bq client
    client = bigquery.Client()

    # Write query results to a new table
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(DATASET).table(TABLE)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query_job = client.query(
        QUERY,
        location=LOCATION, # Location must match dataset
        job_config=job_config)
    rows = list(query_job)  # Waits for the query to finish


    # Export table to GCS
    destination_uri = BUCKET_URI
    dataset_ref = client.dataset(DATASET, project=PROJECT_ID)
    table_ref = dataset_ref.table(TABLE)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location=LOCATION)
    extract_job.result()  # Waits for job to complete