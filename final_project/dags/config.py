# common constants used in all DAGS
# normally would be set thru the environment -placed in the repo just because this is a learning project
# also no secrets are exposed - authentication is done using `gcloud auth login` on the local machine

RAW_BUCKET = 'de07-final-raw'
PROJECT_ID = 'de-07-376021'
LOCATION='EU'
DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 30,
}
