from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator
from table_defs.sales_csv import sales_csv
from config import *
from table_defs.schema_fields import *


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
    project_id=PROJECT_ID,
    dataset_id="bronze",
    table_id="sales",
    schema_fields=SALES_BRONZE_SCHEMA_FIELDS,
    time_partitioning={
        'type': 'DAY',
        'field': '_logical_date',
    }
)

raw_to_bronze = BigQueryInsertJobOperator(
    task_id="raw_to_bronze",
    dag=dag,
    project_id=PROJECT_ID,
    configuration={
        "query": {
            "query": "{% include 'sql/sales_raw_to_bronze.sql' %}",
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
    project_id=PROJECT_ID,
    dataset_id="silver",
    table_id="sales",
    schema_fields=SALES_SILVER_SCHEMA_FIELDS,
    time_partitioning={
        'type': 'DAY',
        'field': 'purchase_date',
    }
)

bronze_to_silver = BigQueryInsertJobOperator(
    task_id="bronze_to_silver",
    dag=dag,
    project_id=PROJECT_ID,
    configuration={
        "query": {
            "query": "{% include 'sql/sales_bronze_to_silver.sql' %}",
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
