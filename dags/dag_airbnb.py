# base
import os
from datetime import datetime


# airflow
from airflow import models
# Google extend
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers import gcs_to_bigquery 
# Postgres extend
from airflow.providers.postgres.operators.postgres import PostgresOperator




# need GOOGLE_APPLICATION_CREDENTIALS = credential keys as env variable
# GOOGLE CLOUD Config
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", 'mlopsmac')
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET_NAME", 'airbnb-mac')
FILE_NAME = 'airbnb-data.csv'
SQL_QUERY = "SELECT * FROM listings;"


with models.DAG(
    dag_id = 'migrate_data_from_postgres_to_gcs',
    start_date = datetime(2021, 1, 1),
    end_data = datetime(2022, 1, 1),
    schedule_interval = '@monthly',
    

) as dag:

    upload_data = PostgresToGCSOperator(
        task_id = 'get_data_from_postgres_to_gcs',
        postgres_conn_id="postgres_default",
        sql = SQL_QUERY,
        bucket = BUCKET_NAME,
        filename = FILE_NAME,
        gzip = False,
    )

    import_in_bigquery = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id = "import_data_in_bq",
        bucket = BUCKET_NAME,
        source_objects = FILE_NAME,
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_TRUNCATE",
        autodetect = True,
        destination_project_dataset_table = 'airbnb.listings', # mlopsmac.airbnb.listing
    )

upload_data >> import_in_bigquery