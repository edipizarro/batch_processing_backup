from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.models import Variable

partition_det_ndet_vars = Variable.get(
    "partition_dets_ndets_config", deserialize_json=True
)


def get_emr_tasks(dag):

    job_flow_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=partition_det_ndet_vars.get("JOB_FLOW_OVERRIDES"),
        aws_conn_id="aws_connection",
        emr_conn_id="emr_default",
        dag=dag,
    )

    job_sensor = EmrJobFlowSensor(
        task_id="check_job_flow",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_connection",
        dag=dag,
    )

    job_flow_creator >> job_sensor
    return [job_flow_creator, job_sensor]
