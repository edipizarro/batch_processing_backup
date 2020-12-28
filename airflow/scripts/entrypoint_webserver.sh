#!/usr/bin/env bash
airflow users create -u admin -r Admin -p admin -f admin -l alerce -e alerce.email@email.com
airflow webserver

