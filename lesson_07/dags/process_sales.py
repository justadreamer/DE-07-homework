from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 30,
}

DATE = "{{ execution_date | ds }}"
BASE_DIR = "/tmp/sales"
RAW_DIR = f'{BASE_DIR}/raw/{DATE}'
STG_DIR = f'{BASE_DIR}/stg/{DATE}'
HEADERS = {'Content-Type':'application/json'}

dag = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

extract_data_from_api = SimpleHttpOperator(
    http_conn_id="job1",
    endpoint="/",
    data=json.dumps({'date': DATE, 'raw_dir': RAW_DIR}),
    headers=HEADERS,
    task_id='extract_data_from_api',
    dag=dag,
)

convert_to_avro = SimpleHttpOperator(
    http_conn_id="job2",
    endpoint="/",
    data=json.dumps({'raw_dir': RAW_DIR, 'stg_dir': STG_DIR}),
    headers=HEADERS,
    task_id='convert_to_avro',
    dag=dag,
)

extract_data_from_api >> convert_to_avro