from airflow import DAG
from emr_dag import get_emr_tasks
from leftraru_dag import get_leftraru_tasks
from airflow.utils.dates import days_ago

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 0,
    "depends_on_past": False,
}

dag = DAG(
    "batch_processing",
    default_args=default_args,
    description="batch process ztf historic data",
    start_date=days_ago(2),
    schedule_interval=None,
)

emr_tasks = get_emr_tasks(dag)
leftraru_tasks = get_leftraru_tasks(dag)
emr_tasks[-1] >> leftraru_tasks[0]
