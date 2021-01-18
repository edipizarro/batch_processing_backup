from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.providers.sftp.sensors.sftp import SFTPSensor

from utils import get_aws_credentials


def get_correction(dag):
    aws_access_key, aws_secret_access_key = get_aws_credentials()
    leftraru_load_vars = Variable.get("leftraru_config", deserialize_json=True)
    partitions = leftraru_load_vars.get("partitions", "")
    input_bucket = leftraru_load_vars.get("detections_uri", "")
    formatter = leftraru_load_vars.get("detections_pattern", "")
    cmd = f"sbatch --array {partitions} correction.slurm {input_bucket} {formatter}"

    execute_correction = SSHOperator(
        task_id="launch_create_csv",
        ssh_conn_id="leftraru_connection",
        command="leftraru_run_job.sh",
        environment={
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        },
        params={
            "aws_access_key": aws_access_key,
            "aws_secret_access_key": aws_secret_access_key,
            "command": cmd,
            "rm": "rm /home/apps/astro/alercebroker/historic_data/detections_corrected*"
        },
        dag=dag,
    )

    correction_sensor = SFTPSensor(
        task_id=f"check_correction_file_created",
        sftp_conn_id="leftraru_ftp_connection",
        path="/home/apps/astro/alercebroker/historic_data/detections_corrected_0.parquet",
        dag=dag,
    )
    execute_correction >> correction_sensor
