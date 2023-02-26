from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from config import *


dag = DAG(
    dag_id='enrich_user_profiles',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022, 9, 1),
)

# the schema still reflects the fact that some values might be missing
# we are pessimistic, because the data in real life might not be ideal
# however we will fill what we can from the silver.user_profiles table
create_gold_table = BigQueryCreateEmptyTableOperator(
    task_id="create_gold_table",
    dag=dag,
    dataset_id="gold",
    table_id="user_profiles_enriched",
    schema_fields=[
        {'name': 'client_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

enrich = BigQueryInsertJobOperator(
    task_id="enrich",
    dag=dag,
    configuration={
        "query": {
            "query": "{% include 'sql/enrich_user_profiles.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID
    }
)

create_gold_table >> enrich
