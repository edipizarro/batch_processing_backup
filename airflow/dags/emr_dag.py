from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.models import Variable


def get_emr_tasks(dag):
    partition_det_ndet_vars = Variable.get(
        "partition_dets_ndets_config", deserialize_json=True
    )
    AVRO_SOURCE_FILES = partition_det_ndet_vars.get("source_avro")
    OUTPUT_DETECTIONS = partition_det_ndet_vars.get("output_detections")
    OUTPUT_NON_DETECTIONS = partition_det_ndet_vars.get("output_non_detections")
    BOOTSTRAP_ACTIONS_SCRIPT = partition_det_ndet_vars.get(
        "bootstrap_actions_script",
        "s3://alerce-static/emr/bootstrap-actions/emr_install_software.sh",
    )
    MASTER_INSTANCE_TYPE = partition_det_ndet_vars.get(
        "master_instance_type", "m5.xlarge"
    )
    MASTER_INSTANCE_COUNT = partition_det_ndet_vars.get("master_instance_count", 1)
    CORE_INSTANCE_TYPE = partition_det_ndet_vars.get("core_instance_type", "m5.4xlarge")
    CORE_INSTANCE_COUNT = partition_det_ndet_vars.get("core_instance_count", 1)
    EC2_KEY_NAME = partition_det_ndet_vars.get("ssh_key", "alerce")
    KEEP_JOB_FLOW_ALIVE = partition_det_ndet_vars.get("keep_job_flow_alive", False)
    TERMINATION_PROTECTED = partition_det_ndet_vars.get("termination_protected", False)
    CLUSTER_NAME = partition_det_ndet_vars.get("cluster_name", "BatchProcessing")
    GROUP_MARKET = partition_det_ndet_vars.get("instance_group_market", "ON_DEMAND")

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
                    "--conf",
                    "spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.5",
                    "/tmp/batch_processing/main.py",
                    "partition-dets-ndets",
                    AVRO_SOURCE_FILES,
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
                    f"--dest={OUTPUT_DETECTIONS}",
                    "--src=hdfs:///user/hadoop/detections/",
                ],
            },
        },
        {
            "Name": "Copy non detections to s3",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "s3-dist-cp",
                    f"--dest={OUTPUT_NON_DETECTIONS}",
                    "--src=hdfs:///user/hadoop/non_detections/",
                ],
            },
        },
    ]

    JOB_FLOW_OVERRIDES = {
        "Name": CLUSTER_NAME,
        "ReleaseLabel": "emr-5.29.0",
        "LogUri": "s3://alerce-airflow-logs/emr",
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "SPOT",
                    "InstanceRole": "MASTER",
                    "InstanceType": MASTER_INSTANCE_TYPE,
                    "InstanceCount": int(MASTER_INSTANCE_COUNT),
                },
                {
                    "Name": "Compute node",
                    "Market": GROUP_MARKET,
                    "InstanceRole": "CORE",
                    "InstanceType": CORE_INSTANCE_TYPE,
                    "InstanceCount": int(CORE_INSTANCE_COUNT),
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": KEEP_JOB_FLOW_ALIVE,
            "TerminationProtected": TERMINATION_PROTECTED,
            "Ec2KeyName": EC2_KEY_NAME,
        },
        "Steps": SPARK_STEPS,
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
        "BootstrapActions": [
            {
                "Name": "Install software",
                "ScriptBootstrapAction": {"Path": BOOTSTRAP_ACTIONS_SCRIPT},
            }
        ],
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
    }

    job_flow_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
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
