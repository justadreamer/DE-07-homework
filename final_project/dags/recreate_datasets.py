from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteDatasetOperator, \
    BigQueryCreateEmptyDatasetOperator
from config import *


dag = DAG(
    dag_id='recreate_datasets',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022, 9, 1),
)

# this is for testing the whole pipeline - to conveniently be able to nuke and recreate datasets
# and rerun all

delete_bronze = BigQueryDeleteDatasetOperator(
    task_id="delete_bronze",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="bronze",
    delete_contents=True,
)

delete_silver = BigQueryDeleteDatasetOperator(
    task_id="delete_silver",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="silver",
    delete_contents=True,
)

delete_gold = BigQueryDeleteDatasetOperator(
    task_id="delete_gold",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="gold",
    delete_contents=True,
)

create_bronze = BigQueryCreateEmptyDatasetOperator(
    task_id="create_bronze",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="bronze",
    location=LOCATION
)

create_silver = BigQueryCreateEmptyDatasetOperator(
    task_id="create_silver",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="silver",
    location=LOCATION
)

create_gold = BigQueryCreateEmptyDatasetOperator(
    task_id="create_gold",
    dag=dag,
    project_id=PROJECT_ID,
    dataset_id="gold",
    location=LOCATION
)

delete_bronze >> create_bronze
delete_silver >> create_silver
delete_gold >> create_gold
