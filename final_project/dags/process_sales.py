from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator
from table_defs.sales_csv import sales_csv


# These params normally would be provided via env or config file, however for study purposes they are here
RAW_BUCKET = 'de07-final-raw'
PROJECT_ID = 'de-07-376021'
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 30,
}


dag = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 10, 1),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

create_bronze_table = BigQueryCreateEmptyTableOperator(
    task_id="create_bronze_table",
    dag=dag,
    dataset_id="bronze",
    table_id="sales",
    schema_fields=[
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'purchase_date', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'product', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': '_logical_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    ],
    time_partitioning={
        'type': 'DAY',
        'field': '_logical_date',
    }
)

raw_to_bronze = BigQueryInsertJobOperator(
    task_id="raw_to_bronze",
    dag=dag,
    configuration={
        "query": {
            "query": "{% include 'sql/raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "sales_csv": sales_csv,
            },
        }
    },
    params={
        'project_id': PROJECT_ID,
        'raw_bucket': RAW_BUCKET
    }
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

create_bronze_table >> \
raw_to_bronze >> \
create_silver_table >> \
bronze_to_silver
