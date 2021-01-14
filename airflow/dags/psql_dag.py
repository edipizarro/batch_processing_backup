from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sensors.sql import SqlSensor
from airflow.models.connection import Connection
from airflow import settings
import re
import logging

import os
import json


def get_create_csv_command(vars):
    command = f"python main.py create-csv --config_json={json.dumps(vars)}"
    return command


def get_psql_copy_command(vars):
    command = f"python main.py psql-copy-csv --config_json={json.dumps(vars)}"
    return command


def get_psql_process_command(vars):
    command = f"python main.py process-csv --config_json={json.dumps(vars)}"
    return command


def get_tables_to_process(vars):
    tables = vars.get("tables").copy()
    for tb in tables.copy().keys():
        if tables[tb] == False:
            del tables[tb]
    return tables


def get_create_csv_tasks(dag):
    psql_load_vars = Variable.get("load_psql_config", deserialize_json=True)
    create_csv_command = get_create_csv_command(psql_load_vars)
    execute_activate_venv = SSHOperator(
        task_id="activate_environment",
        ssh_conn_id="reuna_connection",
        command="conda activate alerce",
        dag=dag,
    )
    execute_create_csv = SSHOperator(
        task_id="launch_create_csv",
        ssh_conn_id="reuna_connection",
        command=create_csv_command,
        dag=dag,
    )

    tables = get_tables_to_process(psql_load_vars)
    sensors = []
    for table in tables.keys():
        success_path = os.path.join(psql_load_vars["outputs"][table], "_SUCCESS")
        create_csv_sensor = SFTPSensor(
            task_id=f"check_{table}_csv_created",
            sftp_conn_id="reuna_ftp_connection",
            path=success_path,
            dag=dag,
        )
        sensors.append(create_csv_sensor)
        execute_create_csv >> create_csv_sensor

    execute_activate_venv >> execute_create_csv
    return [execute_activate_venv, execute_create_csv, *sensors]


def create_conn(psql_db_uri):
    conn = Connection(
        conn_id="ztf_db",
        description="connection for ztf database",
        uri=psql_db_uri,
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    if str(conn_name) == str(conn.conn_id):
        return conn

    session.add(conn)
    session.commit()
    return conn


def get_cat_command(vars, table):
    output = vars["outputs"][table]
    if output[-1] == "/":
        command = f"cat {output}*.csv | wc -l"
    else:
        command = f"cat {output}/*.csv | wc -l"

    return command


def get_psql_db_uri(psql_load_vars):
    user = psql_load_vars["db"]["user"]
    password = psql_load_vars["db"]["password"]
    host = psql_load_vars["db"]["host"]
    port = psql_load_vars["db"]["port"]
    dbname = psql_load_vars["db"]["dbname"]
    psql_db_uri = f"postgresql://{user}:{password}@{host}/{dbname}"
    return psql_db_uri


def get_psql_copy_csv_tasks(dag):
    psql_load_vars = Variable.get("load_psql_config", deserialize_json=True)
    psql_db_uri = get_psql_db_uri(psql_load_vars)
    psql_copy_command = get_psql_copy_command(psql_load_vars)
    execute_activate_venv = SSHOperator(
        task_id="activate_environment",
        ssh_conn_id="reuna_connection",
        command="conda activate alerce",
        dag=dag,
    )
    execute_copy_csv = SSHOperator(
        task_id="launch_psql_copy_csv",
        ssh_conn_id="reuna_connection",
        command=psql_copy_command,
        dag=dag,
    )
    execute_activate_venv >> execute_copy_csv
    tables = get_tables_to_process(psql_load_vars)
    sql_count_tasks = []
    csv_count_tasks = []
    connection = create_conn(psql_db_uri)
    for table in tables:
        cat_command = get_cat_command(psql_load_vars, table)
        count_csv_tuples = SSHOperator(
            task_id=f"count_csv_tuples_{table}",
            ssh_conn_id="reuna_connection",
            command=cat_command,
            do_xcom_push=True,
            dag=dag,
        )
        csv_count_tasks.append(count_csv_tuples)
        sql_command = f"select count(*) from {table} "
        s_aux = f" ti.xcom_pull(task_ids='count_csv_tuples_{table}') "
        sql_command += (
            "having count(*) = {% if "
            + s_aux
            + " %}{{"
            + s_aux
            + ".strip().decode()}}{% endif %}"
        )
        count_psql_tuples = SqlSensor(
            task_id=f"count_sql_tuples_{table}",
            conn_id=connection.conn_id,
            sql=sql_command,
            fail_on_empty=False,
            timeout=60 * 60 * 10,
            dag=dag,
        )
        sql_count_tasks.append(count_psql_tuples)
        execute_copy_csv >> count_csv_tuples >> count_psql_tuples


def get_process_csv_tasks(dag):
    psql_load_vars = Variable.get("load_psql_config", deserialize_json=True)
    psql_db_uri = get_psql_db_uri(psql_load_vars)
    command = get_psql_process_command(psql_load_vars)
    execute_activate_venv = SSHOperator(
        task_id="activate_environment",
        ssh_conn_id="reuna_connection",
        command="conda activate alerce",
        dag=dag,
    )
    execute_process_csv = SSHOperator(
        task_id="launch_process_csv",
        ssh_conn_id="reuna_connection",
        command=command,
        dag=dag,
    )

    tables = get_tables_to_process(psql_load_vars)
    for table in tables.keys():
        success_path = os.path.join(psql_load_vars["outputs"][table], "_SUCCESS")
        create_csv_sensor = SFTPSensor(
            task_id=f"check_{table}_csv_created",
            sftp_conn_id="reuna_ftp_connection",
            path=success_path,
            dag=dag,
        )
        execute_process_csv >> create_csv_sensor

        cat_command = get_cat_command(psql_load_vars, table)
        count_csv_tuples = SSHOperator(
            task_id=f"count_csv_tuples_{table}",
            ssh_conn_id="reuna_connection",
            command=cat_command,
            do_xcom_push=True,
            dag=dag,
        )
        sql_command = f"select count(*) from {table} "
        s_aux = f" ti.xcom_pull(task_ids='count_csv_tuples_{table}') "
        sql_command += (
            "having count(*) = {% if "
            + s_aux
            + " %}{{"
            + s_aux
            + ".strip().decode()}}{% endif %}"
        )
        connection = create_conn(psql_db_uri)
        count_psql_tuples = SqlSensor(
            task_id=f"count_sql_tuples_{table}",
            conn_id=connection.conn_id,
            sql=sql_command,
            fail_on_empty=False,
            timeout=60 * 60 * 10,
            dag=dag,
        )
        create_csv_sensor >> count_csv_tuples >> count_psql_tuples

    execute_activate_venv >> execute_process_csv
