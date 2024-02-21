import json
from pyspark.sql import SparkSession
import os

from .utils import Utils

class ParquetReader:
    """
    ParquetReader class for reading Parquet files using PySpark.

    It assumes a folder structure with MJD-ordered parquet files and provides
    an iterator to return dataframes.
    """
    def __init__(self, config_path: str):
        """
        Initialize the ParquetReader class.

        :param config_path: Path to the configuration file in JSON format.
        """
        # Load json config
        with open(config_path) as config:
            self.config = json.loads(config.read())

        # Configure logger
        self.logger = Utils.configure_logger()

        # Configure Spark
        self.spark = None

    def register_spark_session(self, spark: SparkSession):
        """
        Register a Spark session.

        :param spark: An initialized SparkSession object.
        """
        self.spark = spark

    def get_parquet_mjd_folder(self, mjd: str):
        """
        Get the path to the Parquet folder for a given MJD.

        :param mjd: Modified Julian Date.
        :return: Path to the Parquet folder for the specified MJD.
        """
        return os.path.join(
            self.config["files_base_folder"],
            self.config["parquet_folder"],
            mjd
        )

    def _df_generator(self):
        """
        Private method to generate dataframes from Parquet files.

        :return: Iterator of dataframes.
        """
        date_format = self.config["date_format"]
        start_date = Utils.str_to_date(self.config["start_date"], date_format)
        end_date = Utils.str_to_date(self.config["end_date"], date_format)
        for date in Utils.dates_between_generator(start_date, end_date):
            mjd = Utils.date_to_mjd(date)
            directory = self.get_parquet_mjd_folder(mjd)
            parquet_paths = [os.path.join(directory, avro) for avro in os.listdir(directory)]
            for parquet_path in parquet_paths:
                df = self.spark.read.parquet(parquet_path)
                yield df

    def get_df_generator(self):
        """
        Public method to get the dataframe generator.

        :return: Iterator of dataframes.
        """
        return self._df_generator()
