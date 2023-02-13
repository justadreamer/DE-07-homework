from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable
import os

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 30,
}

DATE = "{{ execution_date | ds }}"
BASE_DIR = Variable.get('BASE_DIR_SALES')

dag = DAG(
    dag_id='upload_to_gcs',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

upload_csv = LocalFilesystemToGCSOperator(
    dag=dag,
    task_id="upload_csv",
    bucket='de07homework',
    src=os.path.join(BASE_DIR, f'{DATE}/*'),
    dst=f'{DATE}/'

)

upload_csv
