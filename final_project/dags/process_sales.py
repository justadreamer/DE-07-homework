from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateExternalTableOperator, \
    BigQueryDeleteTableOperator, \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator


RAW_BUCKET = 'de07-final-raw'
PROJECT_ID = 'de-07-376021'
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 30,
}

dag = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 9, 2),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

delete_bronze_table = BigQueryDeleteTableOperator(
    task_id='delete_bronze_table',
    dag=dag,
    deletion_dataset_table="bronze.sales",
    ignore_if_missing=True,
)

create_bronze_table = BigQueryCreateExternalTableOperator(
    task_id='create_bronze_table',
    dag=dag,
    bucket=RAW_BUCKET,
    # remove day leading 0: 2022-09-01 -> 2022-09-1
    source_objects=["sales/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%-d') }}/*.csv"],
    destination_project_dataset_table="bronze.sales",
    schema_fields=[
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'purchase_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'product', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    source_format="CSV",
    skip_leading_rows=1,
    field_delimiter=",",
)

create_silver_table = BigQueryCreateEmptyTableOperator(
    task_id="create_silver_table",
    dag=dag,
    dataset_id="silver",
    table_id="sales",
    schema_fields=[
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'purchase_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'product', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    ],
    time_partitioning={
        'type': 'DAY',
        'field': 'purchase_date',
    }
)

bronze_to_silver = BigQueryInsertJobOperator(
    task_id="bronze_to_silver",
    dag=dag,
    configuration={
        "query": {
            "query": "{% include 'sql/bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID
    }
)

delete_bronze_table >> \
create_bronze_table >> \
create_silver_table >> \
bronze_to_silver
