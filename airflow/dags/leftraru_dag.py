from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from sensors.ssh_command_sensor import SSHCommandSensor

from utils import get_aws_credentials, get_tables_to_process


def get_leftraru_tasks(dag):
    aws_access_key, aws_secret_access_key = get_aws_credentials()
    leftraru_compute_vars = Variable.get(
        "leftraru_compute_config", deserialize_json=True
    )

    check_job_script_path = leftraru_compute_vars.get(
        "check_job_script_path",
        "/home/apps/astro/alercebroker/batch_processing/computing/scripts/check_finished.sh",
    )

    tables = get_tables_to_process(leftraru_compute_vars)

    for i, table in enumerate(tables):
        partitions = leftraru_compute_vars["partitions"][table]
        input_bucket = leftraru_compute_vars["inputs"][table]
        input_pattern = leftraru_compute_vars["input_patterns"][table]
        output_dir = leftraru_compute_vars["outputs"][table]
        log_dir = leftraru_compute_vars["log_dirs"][table]

        execute = SSHOperator(
            task_id=f"launch_slurm_script_for_{table}",
            ssh_conn_id="leftraru_connection",
            command="leftraru_run_job.sh",
            environment={
                "AWS_ACCESS_KEY_ID": aws_access_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
            },
            params={
                "output_dir": output_dir,
                "aws_access_key": aws_access_key,
                "aws_secret_access_key": aws_secret_access_key,
                "partitions": partitions,
                "script_name": f"{table}.slurm",
                "input_parquet": input_bucket,
                "input_pattern": input_pattern,
                "log_dir": log_dir,
                "output_dir": output_dir,
            },
            do_xcom_push=True,
            dag=dag,
        )

        sensor = SSHCommandSensor(
            task_id=f"check_{table}_files_created",
            ssh_conn_id="leftraru_connection",
            command="leftraru_check_job.sh",
            dag=dag,
        )
        execute >> sensor
