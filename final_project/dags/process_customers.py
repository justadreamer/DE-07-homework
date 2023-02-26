from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from config import *
from table_defs.schema_fields import *


dag = DAG(
    dag_id='process_customers',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 6),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

transfer_customers_to_bronze = GCSToBigQueryOperator(
    task_id='transfer_customers_to_bronze',
    dag=dag,
    bucket=RAW_BUCKET,
    source_objects=["customers/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%-d') }}/*.csv"],
    destination_project_dataset_table='bronze.customers',
    schema_fields=CUSTOMERS_BRONZE_SCHEMA_FIELDS,
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    autodetect=False,
)

create_silver_table = BigQueryCreateEmptyTableOperator(
    task_id="create_silver_table",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="silver",
    table_id="customers",
    schema_fields=CUSTOMERS_SILVER_SCHEMA_FIELDS
)

bronze_to_silver = BigQueryInsertJobOperator(
    task_id="bronze_to_silver",
    dag=dag,
    project_id=PROJECT_ID,
    configuration={
        "query": {
            "query": "{% include 'sql/customers_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID
    }
)

transfer_customers_to_bronze >> \
create_silver_table >> \
bronze_to_silver
