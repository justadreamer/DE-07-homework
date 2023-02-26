from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

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
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
)

create_silver_table = BigQueryCreateEmptyTableOperator(
    task_id="create_silver_table",
    dag=dag,
    dataset_id="silver",
    table_id="customers",
    schema_fields=[
        {'name': 'client_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

bronze_to_silver = BigQueryInsertJobOperator(
    task_id="bronze_to_silver",
    dag=dag,
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
