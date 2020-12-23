from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

SPARK_STEPS = [
    {
        "Name": "Partition Detections and Non Detections",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--conf",
                "spark.pyspark.python=/usr/bin/python3.6",
                "/tmp/batch_processing/main.py",
                "partition-dets-ndets",
                "s3a://ztf-avro/ztf_20180601_programid1/*.avro",
                "detections",
                "non_detections",
                "/tmp/batch_processing/partition_avro/alert.avsc",
            ],
        },
    },
    {
        "Name": "Copy detections to s3",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--dest=s3://ztf-historic-data/airflowtest/detections/",
                "--src=hdfs:///detections",
            ],
        },
    },
    {
        "Name": "Copy detections to s3",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--dest=s3://ztf-historic-data/airflowtest/non-detections/",
                "--src=hdfs:///non_detections",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "AirflowTest",
    "ReleaseLabel": "emr-5.29.0",
    "LogUri": "s3://alerce-airflow-logs/emr",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Compute node",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2KeyName": "alerce",
    },
    "Steps": SPARK_STEPS,
    "BootstrapActions": [
        {
            "Name": "Install software",
            "ScriptBootstrapAction": {
                "Path": "s3://alerce-static/emr/bootstrap-actions/emr_install_software.sh"
            },
        }
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


def get_emr_tasks(dag):
    job_flow_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
    )

    job_sensor = EmrJobFlowSensor(
        task_id="check_job_flow",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        dag=dag,
    )

    job_flow_creator >> job_sensor
    return [job_flow_creator, job_sensor]
