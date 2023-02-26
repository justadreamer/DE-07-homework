from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator
from config import *
from table_defs.schema_fields import *


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
    project_id=PROJECT_ID,
    dataset_id="gold",
    table_id="user_profiles_enriched",
    schema_fields=USER_PROFILES_ENRICHED_SCHEMA,
)

enrich = BigQueryInsertJobOperator(
    task_id="enrich",
    dag=dag,
    project_id=PROJECT_ID,
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
