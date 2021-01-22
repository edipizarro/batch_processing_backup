from airflow import settings
from airflow.models.connection import Connection
from urllib.parse import urlparse


def get_aws_credentials():
    session = settings.Session()
    conn = (
        session.query(Connection).filter(Connection.conn_id == "aws_connection").first()
    )
    parsed = urlparse(conn.get_uri())
    credentials = parsed.netloc.split(":")
    access_key = credentials[0]
    secret_access_key = credentials[1][:-1]
    return access_key, secret_access_key


def get_tables_to_process(vars):
    tables = vars.get("tables").copy()
    for tb in tables.copy().keys():
        if tables[tb] == False:
            del tables[tb]
    return tables
