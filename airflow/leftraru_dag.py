from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
import os

leftraru_host = os.environ["LEFTRARU_HOST"]
leftraru_username = os.environ["LEFTRARU_USER"]
leftraru_password = os.environ["LEFTRARU_PASSWORD"]

leftraru_hook = SSHHook(
    remote_host=leftraru_host,
    username=leftraru_username,
    password=leftraru_password,
)


def get_leftraru_tasks(dag):
    run_process_detections = SSHOperator(
        task_id="process_detections", command="date", dag=dag
    )
    return [run_process_detections]
