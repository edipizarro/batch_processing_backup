#!/usr/bin/env bash
airflow users create -u $ADMIN_USER -r Admin -p $ADMIN_PASSWORD -f admin -l alerce -e alerce.email@email.com
airflow variables import /opt/airflow/variables/variables.json
airflow connections add reuna_connection --conn-description="connect to reuna through ssh" --conn-host="alerce.reuna.cl" --conn-port="22" --conn-type="ssh"
airflow connections add reuna_ftp_connection --conn-description="SFTP connection for reuna" --conn-host="alerce.reuna.cl" --conn-port="22" --conn-type="sftp"
airflow connections add ztf_db --conn-description="ZTF DB Connection" --conn-type="postgres"
airflow connections add aws_connection --conn-description="AWS Connection" --conn-type="aws"
airflow webserver
