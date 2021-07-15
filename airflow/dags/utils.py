from airflow import settings
from airflow.models.connection import Connection
from urllib.parse import urlparse, parse_qs


def get_aws_credentials(conn_id):
    session = settings.Session()
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    parsed = urlparse(conn.get_uri())
    credentials = parsed.netloc.split(":")
    access_key = credentials[0]
    secret_access_key = credentials[1][:-1]
    token = parse_qs(parsed.query)["aws_session_token"]
    return access_key, secret_access_key, token


def get_tables_to_process(vars):
    tables = vars.get("tables").copy()
    for tb in tables.copy().keys():
        if tables[tb] == False:
            del tables[tb]
    return tables


class S3Url(object):
    """
    >>> s = S3Url("s3://bucket/hello/world")
    >>> s.bucket
    'bucket'
    >>> s.key
    'hello/world'
    >>> s.url
    's3://bucket/hello/world'

    >>> s = S3Url("s3://bucket/hello/world?qwe1=3#ddd")
    >>> s.bucket
    'bucket'
    >>> s.key
    'hello/world?qwe1=3#ddd'
    >>> s.url
    's3://bucket/hello/world?qwe1=3#ddd'

    >>> s = S3Url("s3://bucket/hello/world#foo?bar=2")
    >>> s.key
    'hello/world#foo?bar=2'
    >>> s.url
    's3://bucket/hello/world#foo?bar=2'
    """

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip("/") + "?" + self._parsed.query
        else:
            return self._parsed.path.lstrip("/")

    @property
    def url(self):
        return self._parsed.geturl()

    def add_prefix(self, prefix):
        if self.key[-1] == "/":
            return self.key + prefix
        else:
            return self.key + "/" + prefix
