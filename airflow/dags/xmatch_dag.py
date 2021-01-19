from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

xmatch_vars = Variable.get("compute_xmatch_config", deserialize_json=True)

OBJECT_SOURCE_FILES = xmatch_vars.get("source_objects")
OUTPUT_FILES = xmatch_vars.get("output_xmatch")
CATALOG_SOURCE_FILES = xmatch_vars.get("source_catalog")
BOOTSTRAP_ACTIONS_SCRIPT = xmatch_vars.get(
    "bootstrap_actions_script",
    "s3://alerce-static/emr/bootstrap-actions/xmatch_emr_install_software.sh",
)

MASTER_INSTANCE_TYPE = xmatch_vars.get("master_instance_type", "m5.xlarge")
MASTER_INSTANCE_COUNT = xmatch_vars.get("master_instance_count", 1)
CORE_INSTANCE_TYPE = xmatch_vars.get("core_instance_type", "m5.4xlarge")
CORE_INSTANCE_COUNT = xmatch_vars.get("core_instance_count", 1)
EC2_KEY_NAME = xmatch_vars.get("ssh_key", "alerce")
KEEP_JOB_FLOW_ALIVE = xmatch_vars.get("keep_job_flow_alive", False)
TERMINATION_PROTECTED = xmatch_vars.get("termination_protected", False)
CLUSTER_NAME = xmatch_vars.get("cluster_name", "BatchProcessingXmatch")
GROUP_MARKET = xmatch_vars.get("instance_group_market", "ON_DEMAND")


def get_xmatch_tasks(dag):
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
                    "xmatch",
                    OBJECT_SOURCE_FILES,
                    "xmatch",
                    CATALOG_SOURCE_FILES,
                ],
            },
        },
        {
            "Name": "Copy xmatch to s3",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "s3-dist-cp",
                    f"--dest={OUTPUT_FILES}",
                    "--src=hdfs:///user/hadoop/xmatch/",
                ],
            },
        },
    ]

    JOB_FLOW_OVERRIDES = {
        "Name": CLUSTER_NAME,
        "ReleaseLabel": "emr-5.29.0",
        "LogUri": "s3://alerce-airflow-logs/xmatch_emr",
        "Configurations": [
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.jars": "/tmp/minimal_astroide.jar,/tmp/healpix-1.0.jar"
                },
            }
        ],
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
