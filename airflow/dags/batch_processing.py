from airflow import DAG
from partition_dag import get_emr_tasks
from psql_dag import (
    get_create_csv_tasks,
    get_psql_copy_csv_tasks,
    get_process_csv_tasks,
)
from xmatch_dag import get_xmatch_tasks
from airflow.utils.dates import days_ago


default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 0,
    "depends_on_past": False,
}

partition_dets_ndets_dag = DAG(
    "partition_avro",
    default_args=default_args,
    description="partition ztf avro files into parquet",
    start_date=days_ago(2),
    schedule_interval=None,
)

create_csv_dag = DAG(
    "create_csv",
    default_args=default_args,
    description="create_csv in reuna",
    start_date=days_ago(2),
    schedule_interval=None,
    template_searchpath="/opt/airflow/templates",
)

psql_copy_csv_dag = DAG(
    "psql_copy_csv",
    default_args=default_args,
    description="copy csv to psql db in reuna",
    start_date=days_ago(2),
    schedule_interval=None,
    template_searchpath="/opt/airflow/templates",
)

psql_create_and_copy_csv_dag = DAG(
    "psql_create_and_copy_csv",
    default_args=default_args,
    description="create and copy csv to psql db in reuna",
    start_date=days_ago(2),
    schedule_interval=None,
    template_searchpath="/opt/airflow/templates",
)

compute_xmatch_dag = DAG(
    "compute_xmatch",
    default_args=default_args,
    description="Compute xmatch in emr",
    start_date=days_ago(2),
    schedule_interval=None,
)

# dag = DAG(
#     "batch_processing",
#     default_args=default_args,
#     description="batch process ztf historic data",
#     start_date=days_ago(2),
#     schedule_interval=None,
# )

emr_tasks_exclusive = get_emr_tasks(partition_dets_ndets_dag)
create_csv_tasks = get_create_csv_tasks(create_csv_dag)
psql_copy_csv_tasks = get_psql_copy_csv_tasks(psql_copy_csv_dag)
process_csv_tasks = get_process_csv_tasks(psql_create_and_copy_csv_dag)
xmatch_tasks = get_xmatch_tasks(compute_xmatch_dag)
