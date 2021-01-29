from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from sensors.ssh_command_sensor import SSHCommandSensor
from utils import get_aws_credentials, get_tables_to_process, S3Url


def get_leftraru_tasks(dag):
    aws_access_key, aws_secret_access_key = get_aws_credentials("aws_connection")
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
        input_parquets = leftraru_compute_vars["inputs"][table]
        input_pattern = leftraru_compute_vars["input_patterns"][table]
        local_output_dir = leftraru_compute_vars["local_outputs"][table]
        s3_output_dir = leftraru_compute_vars["s3_outputs"][table]
        log_dir = leftraru_compute_vars["log_dirs"][table]
        s3_url = S3Url(s3_output_dir)

        execute = SSHOperator(
            task_id=f"launch_slurm_script_for_{table}",
            ssh_conn_id="leftraru_connection",
            command="leftraru_run_job.sh",
            environment={
                "AWS_ACCESS_KEY_ID": aws_access_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
            },
            params={
                "rm": True,
                "output_dir": local_output_dir,
                "aws_access_key": aws_access_key,
                "aws_secret_access_key": aws_secret_access_key,
                "partitions": partitions,
                "script_name": table,
                "input_parquet": input_parquets,
                "input_pattern": input_pattern,
                "log_dir": log_dir,
            },
            do_xcom_push=True,
            dag=dag,
        )

        sensor = SSHCommandSensor(
            task_id=f"check_{table}_files_created",
            ssh_conn_id="leftraru_connection",
            command="leftraru_check_job.sh",
            params={"table": table, "task_name": f"launch_slurm_script_for_{table}"},
            dag=dag,
        )

        s3_upload = SSHOperator(
            task_id=f"upload_{table}_to_s3",
            ssh_conn_id="leftraru_connection",
            command="leftraru_run_job.sh",
            environment={
                "AWS_ACCESS_KEY_ID": aws_access_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
            },
            params={
                "rm": False,
                "output_dir": s3_output_dir,
                "aws_access_key": aws_access_key,
                "aws_secret_access_key": aws_secret_access_key,
                "partitions": partitions,
                "script_name": "upload_s3",
                "input_parquet": local_output_dir,
                "input_pattern": table,
                "log_dir": log_dir,
            },
            do_xcom_push=True,
            dag=dag,
        )

        sensor_s3_upload_finish = SSHCommandSensor(
            task_id=f"check_{table}_files_uploaded",
            ssh_conn_id="leftraru_connection",
            command="leftraru_check_job.sh",
            params={"task_name": f"upload_{table}_to_s3"},
            dag=dag,
        )

        sensor_s3_files_uploaded = S3PrefixSensor(
            bucket_name=s3_url.bucket,
            prefix=s3_url.add_prefix(table + "*"),
            aws_conn_id="aws_connection",
            dag=dag,
        )

        execute >> sensor >> s3_upload >> sensor_s3_upload_finish >> sensor_s3_files_uploaded
