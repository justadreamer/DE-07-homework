from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

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
    start_date=datetime(2023, 1, 9),
    end_date=datetime(2023, 1, 11),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

extract_data_from_api = SimpleHttpOperator(
    task_id='extract_data_from_api',
    dag=dag,
)

convert_to_avro = SimpleHttpOperator(
    task_id='convert_to_avro',
    dag=dag,
)

extract_data_from_api >> convert_to_avro