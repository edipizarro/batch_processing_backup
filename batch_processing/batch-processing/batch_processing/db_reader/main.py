import json
import os

import polars
from pymongo import MongoClient

from batch_utils import  (
    configure_logger,
    date_to_mjd,
    str_to_date,
    dates_between_generator,
    _init_path
)

class DBParquetBase:
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        Initialize the ParquetReader class.

        :param config_path: Path to the configuration file in JSON format.
        """

        if config_path and config_dict:
            raise TypeError("config_path or config_dict should be given, not both")
        elif config_path: # Load json config
            with open(config_path) as config:
                self.config = json.loads(config.read())
        elif config_dict:
            self.config = config_dict
        else:
            raise TypeError("config_path or config_dict should be given")

        # Configure logger
        self.logger = configure_logger()
    
    def get_base_db_parquet_path(self) -> str:
        driver = self.config["DBCredentials"]["DBDriver"]
        if driver == "mongodb":
            folder = "MongoParquet"
        elif "PSQLParquet":
            folder = "PSQLParquet"
        else:
            raise ValueError(f"Invalid db driver: {driver}")
        return os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"][folder]
        )   

class DBParquetWriter(DBParquetBase):
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        Initialize the DBParquetWriter class.

        :param config_path: Path to the configuration file in JSON format.
        """
        super().__init__(config_path, config_dict)

    def get_downloads_mjd_file_path(self, mjd, compression = None) -> str:
        """
        Get the path where downloaded files for a specific MJD will be saved.

        :param mjd: Modified Julian Date.
        :return: Base path of downloads for the specified MJD.
        """
        base_folder = self.get_base_db_parquet_path()
        _init_path(base_folder)
        if compression:
            compression = "." + compression
        return os.path.join(base_folder, f"{mjd}{compression}.parquet")
    
    def db_uri(self):
        user = self.config["DBCredentials"]["DBUser"]
        password = self.config["DBCredentials"]["DBPassword"]
        host = self.config["DBCredentials"]["DBHost"]
        port = self.config["DBCredentials"]["DBPort"]
        db_name = self.config["DBCredentials"]["DBName"]
        driver = self.config["DBCredentials"]["DBDriver"]

        uri = f"{driver}://{user}:{password}@{host}:{port}/{db_name}"

        auth_source = self.config["DBCredentials"].get("DBAuthSource", None)
        if auth_source:
            uri += f"?authSource={auth_source}"

        return uri

    def execute_db_operation(self, mongo_code_block, postgresql_code_block):
        driver = self.config["DBCredentials"]["DBDriver"]
        code_block = None

        if driver == "mongodb":
            code_block = mongo_code_block
        elif driver == "postgresql":
            code_block = postgresql_code_block
        else:
            raise ValueError(f"Invalid db driver: {driver}")

        if code_block == None:
            raise ValueError(f"No code provided for driver {driver}")

        return code_block()

    def mongo_client(self):
        return self.execute_db_operation(
            mongo_code_block        = lambda: MongoClient(self.db_uri()),
            postgresql_code_block   = None
        )

    def df_from_query(self, query: str, mongo_collection: str = None):
        return self.execute_db_operation(
            mongo_code_block        = lambda: self._query_mongodb(query, mongo_collection),
            postgresql_code_block   = lambda: self._query_postgresql(query)
        )

    def _query_postgresql(self, query: str):
        return polars.read_database_uri(query, self.db_uri())

    def _query_mongodb(self, query: str, mongo_collection: str):
        if mongo_collection == None:
            raise ValueError("Invalid mongo_collection = None")
        client = self.mongo_client()
        db_name = self.config["DBCredentials"]["DBName"]
        db = client[db_name]
        collection = db[mongo_collection]
        cursor = collection.find(
            filter = query["filter"],
            projection = query.get("projection", None)
        )
        data = list(cursor)
        return polars.DataFrame(data)

    def object_query(
            self,
            start_firstmjd: str,
            last_firstmjd: str,
            columns = ["oid", "aid", "firstmjd"]
        ):
        return self.execute_db_operation(
            mongo_code_block        = lambda: self._mongo_object_query(start_firstmjd, last_firstmjd, columns),
            postgresql_code_block   = lambda: self._postgresql_object_query(start_firstmjd, last_firstmjd, columns)
        )

    def _mongo_object_query(
            self,
            start_firstmjd: str,
            last_firstmjd: str,
            columns = ["aid", "oid", "firstmjd"]
        ) -> dict:
        filter = { "firstmjd": { "$gte": int(start_firstmjd), "$lt": int(last_firstmjd) } }
        projection = { col: True for col in columns }
        if projection:
            projection.update({"_id": False})
        return {
            "filter": filter,
            "projection": projection
        }

    def _postgresql_object_query(
            self,
            start_firstmjd: str,
            last_firstmjd: str,
            columns = ["aid", "oid", "firstmjd"]
        ) -> str:
        return f'''
            SELECT
                { ", ".join(columns) }
            FROM
                object
            WHERE
                firstmjd >= {start_firstmjd}
                AND firstmjd < {last_firstmjd}
        '''

    def write_parquet_mjd(self, df, mjd):
        compression = self.config.get("ParquetCompression", "snappy")
        compression_level = 1 if compression == "zstd" else None
        file_path = self.get_downloads_mjd_file_path(mjd, compression)
        df.write_parquet(file_path, compression=compression, compression_level=compression_level)
        self.logger.info(f"Written db parquet for mjd {mjd}")
    
    def execute(self):
        """
        Execute the DB data retrieval.
        """
        date_format = self.config["DateFormat"]
        start_date = str_to_date(self.config["StartDate"], date_format)
        end_date = str_to_date(self.config["EndDate"], date_format)
        for date in dates_between_generator(start_date, end_date):
            mjd = date_to_mjd(date)
            start_firstmjd = mjd
            last_firstmjd = str(int(mjd) + 1)
            query = self.object_query(start_firstmjd, last_firstmjd)
            df = self.df_from_query(query, mongo_collection="object")
            #! ELIMINAR EXPLODE DESPUÉS DE ACTUALIZAR MONGODB
            df = df.explode("oid") # Create one row per oid
            df = df.sort(["oid", "firstmjd", "aid"], descending=False)
            self.write_parquet_mjd(df, mjd)


class DBParquetReader(DBParquetBase):
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        Initialize the DBParquetReader class.

        :param config_path: Path to the configuration file in JSON format.
        """
        super().__init__(config_path, config_dict)

    def _get_all_parquet_path(self):
        base_folder = self.get_base_db_parquet_path()
        wildcard_files_uri = os.path.join(base_folder, "**","*.parquet")
        return wildcard_files_uri

    def lazy_df(self):
        source = self._get_all_parquet_path()
        df = polars.scan_parquet(source, rechunk=True)
        return df
    
    def df_many(self, column: str, values: list):
        lazy_df = self.lazy_df()
        df = lazy_df.filter(polars.col(column).is_in(values)).collect()
        return df
    
    def find_one_in_df(self, df, column: str, value):
        row = df.filter(polars.col(column) == value)
        return row

    def df_sample(self, n = 1, column = "*"):
        df = self.lazy_df()
        return df.select(polars.col(column)).collect().sample(n)

    def len(self):
        df = self.lazy_df()
        return df.select(polars.len()).collect().item()
    
    def mapped_data(self, key_column: str, value_column: str, key_values: list) -> dict:
        mapped_data = {}
        df = self.df_many(key_column, key_values)
        list_of_dicts = df.to_dicts()
        for row in list_of_dicts:
            mapped_data[row[key_column]] = row[value_column]
        return mapped_data
