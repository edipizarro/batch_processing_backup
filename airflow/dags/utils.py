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

