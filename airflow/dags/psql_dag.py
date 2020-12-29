from airflow import DAG
from emr_dag import get_emr_tasks
from airflow.utils.dates import days_ago

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 0,
    "depends_on_past": False,
}

dag = DAG(
    "psql_upload",
    default_args=default_args,
    description="batch process ztf historic data",
    start_date=days_ago(2),
    schedule_interval=None,
)
