from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from config import *


dag = DAG(
    dag_id='process_user_profiles',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022, 9, 1),
)

# since data is of ideal quality we can rely fully on autodetect for schema detection
transfer_user_profiles_to_silver = GCSToBigQueryOperator(
        task_id='transfer_user_profiles_to_silver',
        dag=dag,
        bucket=RAW_BUCKET,
        source_objects=["user_profiles/*.jsonl"],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='silver.user_profiles',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
)

transfer_user_profiles_to_silver
