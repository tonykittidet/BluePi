# Import packages
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

# Define default arguments
default_args = {
    'owner': 'Kittidet S.',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Define dag variables
project_id = 'bluepi-302603'
staging_dataset = 'staging_dataset'
dwh_dataset = 'dwh_dataset'
gs_bucket = 'bluepi_datalake'
des_dir = "BluePi"

# Define dag
dag = DAG('BluePi_Airflow_01',
          start_date=datetime.now(),
          schedule_interval='@hourly',
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)


# Load data from Postgres to GCS

postgres_user_log_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_user_log_to_gcs",
    bucket=gs_bucket,
    filename=des_dir + "/user_log.json",
    sql='''SELECT * FROM public.user_log;''',
    retries=3,
    postgres_conn_id="BluePi_DB"
)




# Load data from GCS to BQ
load_user_log = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_user_log',
    bucket = gs_bucket,
    source_objects = [des_dir + '/user_log.json'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.user_log',
    schema_object = [
 {
   "description": "created_at",
   "name": "created_at",
   "type": "TIMESTAMP",
   "mode": "NULLABLE"
 },
 {
   "description": "updated_at",
   "name": "updated_at",
   "type": "TIMESTAMP",
   "mode": "NULLABLE"
 }, 
 {
   "description": "id",
   "name": "id",
   "type": "STRING",
   "mode": "REQUIRED"
 },
 {
   "description": "user_id",
   "name": "user_id",
   "type": "STRING",
   "mode": "REQUIRED"
 },
 {
   "description": "action",
   "name": "action",
   "type": "STRING",
   "mode": "REQUIRED"
 },
 {
   "description": "status",
   "name": "status",
   "type": "INT64",
   "mode": "NULLABLE"
 }
],
    write_disposition='WRITE_TRUNCATE',
    source_format = 'NEWLINE_DELIMITED_JSON'
)


create_User_log_data = BigQueryOperator(
    task_id = 'create_User_log_data',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/USER_LOG_DATA.sql'
)




finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

dag >> start_pipeline >> postgres_user_log_to_gcs >> load_user_log >> create_User_log_data >> finish_pipeline
