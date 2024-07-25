import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import polars

import os

from batch_utils import  (
    date_to_mjd,
    str_to_date,
    dates_between_generator,
    configure_logger,
    path_exists,
)
from batch_utils.types import ParquetFolder

class ParquetBaseReader:
    """
    ParquetReader class for reading Parquet files using PySpark.

    It assumes a folder structure with MJD-ordered parquet files and provides
    an iterator to return dataframes.
    """
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
        
        # Configure S3
        self.s3 = boto3.client('s3')

        # Configure logger
        self.logger = configure_logger()
    
    def data_folder_path(
            self,
            s3: bool = False,
            data_folder: ParquetFolder = ParquetFolder.SortingHatParquet
        ):
        if s3:
            base = self.config["S3Bucket"]
        else:
            base = self.config["DataFolder"]
        return os.path.join(base, self.config["SubDataFolder"][data_folder.name])
        

    def mjd_folder_path(
            self,
            mjd: str,
            data_folder: ParquetFolder = ParquetFolder.SortingHatParquet
        ):
        """
        Get the path to the Parquet folder for a given MJD.

        :param mjd: Modified Julian Date.
        :return: Path to the Parquet folder for the specified MJD.
        """
        return os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"][data_folder.name],
            mjd
        )
    
    def all_parquet_path_in_mjd(
            self,
            mjd: str,
            data_folder: ParquetFolder = ParquetFolder.SortingHatParquet
    ):
        parquet_path = os.path.join(data_folder, mjd, "**", "*.parquet")
        return parquet_path

    def parquet_id_folder_path(
            self,
            mjd: str,
            id: str,
            data_folder:ParquetFolder = ParquetFolder.SortingHatParquet
        ):
        """
        Get the path to the Parquet file for a given MJD and ID.

        :param mjd: Modified Julian Date.
        :param id: Parquet file ID in given MJD.
        :return: Path to the Parquet folder for the specified MJD.
        :raises FileNotFoundError: If the parquet file does not exist.
        """
        parquet_file = os.path.join(self.mjd_folder_path(mjd, data_folder), id)
        if not os.path.exists(parquet_file):
            raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
        return parquet_file

    def check_and_download_folder(self, local_folder_path: str, s3_folder_path: str):
        """
        Checks for files and folders in a local folder and downloads missing files from S3.
        """
        bucket=self.config["S3Bucket"]

        # Create the local folder if it doesn't exist
        os.makedirs(local_folder_path, exist_ok=True)
        
        # Get a list of files and folders in the S3 folder
        s3_objects = self.s3.list_objects_v2(Bucket=bucket, Prefix=s3_folder_path)['Contents']
        # s3_files = [obj['Key'] for obj in s3_objects]  # Extract filenames from the S3 objects
        s3_base_names = [os.path.basename(obj['Key']) for obj in s3_objects]  # Extract base names from S3 objects

        local_files = set(os.listdir(local_folder_path))
        missing_files = set(s3_base_names) - local_files

        for filename in missing_files:
            local_filepath = os.path.join(local_folder_path, filename)
            s3_filepath = os.path.join(s3_folder_path, filename)
            self.s3.download_file(bucket, s3_filepath, local_filepath)

class ParquetBatchReader(ParquetBaseReader):
    """
    ParquetReader class for reading Parquet files using PySpark.

    It assumes a folder structure with MJD-ordered parquet files and provides
    an iterator to return dataframes.
    """
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        Initialize the ParquetReader class.

        :param config_path: Path to the configuration file in JSON format.
        """
        super().__init__(config_path, config_dict)

        # Configure Spark
        self.spark = None

    def register_spark_session(self, spark: SparkSession):
        """
        Register a Spark session.

        :param spark: An initialized SparkSession object.
        """
        self.spark = spark

    def _df_generator(
            self,
            data_folder: ParquetFolder = ParquetFolder.SortingHatParquet,
            iterate_ids: bool = False):
        """
        Private method to generate dataframes from Parquet files.

        :return: Iterator of dataframes.
        """
        DateFormat = self.config["DateFormat"]
        StartDate = str_to_date(self.config["StartDate"], DateFormat)
        EndDate = str_to_date(self.config["EndDate"], DateFormat)
        for date in dates_between_generator(StartDate, EndDate):
            mjd = date_to_mjd(date)
            directory = self.mjd_folder_path(mjd, data_folder)
            if iterate_ids:
                parquet_paths = [os.path.join(directory, avro) for avro in os.listdir(directory)]
                for parquet_path in parquet_paths:
                    df = self.spark.read.parquet(parquet_path)
                    yield df
            else:
                parquet_path = os.path.join(directory, "**", "*.parquet")
                df = self.spark.read.parquet(parquet_path)
                yield df

    def get_df_generator(self, data_folder: ParquetFolder = ParquetFolder.SortingHatParquet):
        """
        Public method to get the dataframe generator.

        :return: Iterator of dataframes.
        """
        return self._df_generator(data_folder)

    def df(
            self,
            s3: bool = False,
            data_folder: ParquetFolder = ParquetFolder.SortingHatParquet
        ):
        """
        Public method to get one dataframe with all the data.
        :param path: List of paths with parquets to read
        :return: Spark dataframe
        """
        StartDate = str_to_date(self.config["StartDate"], self.config["DateFormat"])
        EndDate = str_to_date(self.config["EndDate"], self.config["DateFormat"])
        data_folder = self.data_folder_path(s3, data_folder)
        parquet_paths = []
        for date in dates_between_generator(StartDate, EndDate):
            mjd = date_to_mjd(date)
            wild_mjd_path = self.all_parquet_path_in_mjd(mjd, data_folder)
            if path_exists(wild_mjd_path, s3, self.spark.sparkContext):
                parquet_paths.append(wild_mjd_path)
            else:
                self.logger.info(f"Path does not exists: {wild_mjd_path}")
        # parquet_paths = [
        #     self.all_parquet_path_in_mjd(mjd, data_folder)
        #     for mjd in map(date_to_mjd, list(dates_between_generator(StartDate, EndDate)))
        #     if path_exists(
        #         self.all_parquet_path_in_mjd(mjd, data_folder),
        #         s3,
        #         self.spark.sparkContext
        #     )
        # ]
        df = self.spark.read.parquet(*parquet_paths)
        return df

class ParquetQueryReader(ParquetBaseReader):
    """
    ParquetReader class for reading Parquet files using PySpark.

    It assumes a folder structure with MJD-ordered parquet files and provides
    an iterator to return dataframes.
    """
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        Initialize the ParquetReader class.

        :param config_path: Path to the configuration file in JSON format.
        """
        super().__init__(config_path, config_dict)

    def get_rows_by_values(self, df, column_name, values):
        """
        Queries the PySpark DataFrame for rows where the specified column contains
        any of the values in the given list.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to query.
            column_name (str): The name of the column to filter by.
            values (list): A list of values to search for.

        Returns:
            pyspark.sql.DataFrame: A new DataFrame containing the matching rows.
        """
        if isinstance(values, list):
            return df.filter(col(column_name).isin(values))
        else:
            return df.filter(col(column_name) == values)

    def df_one(
            self,
            parquet_id: str,
            filter_values: list[str],
            filter_column: str = "candid",
            selected_columns: list[str] = ["*"],
            data_folder: ParquetFolder = ParquetFolder.SortingHatParquet,
            s3: bool = False,
            mjd: str = None
        ):
        """
        Filters a PySpark DataFrame based on values in a specific column of a Parquet file.

        Args:
            :param mjd: Modified Julian Date.
            :param parquet_id: Parquet file ID in given MJD.
            :column_name: Name of the column to filter by.
            :values: A list of values to search for.

        Returns:
            pyspark.sql.DataFrame: A new DataFrame containing the matching rows.
        """
        if not filter_values:
            raise ValueError("List of values to filter should not be empty")

        if not mjd:
            date = str_to_date(self.config["StartDate"], self.config["DateFormat"])
            mjd = date_to_mjd(date)

        source = os.path.join(
            self.parquet_id_folder_path(mjd, parquet_id, data_folder),
            "*.parquet"
        )
        df = polars.scan_parquet(source, rechunk=True).select(*selected_columns)
        if not filter_values:
            raise ValueError
        elif len(filter_values) == 1:
            df = df.filter(polars.col(filter_column) == filter_values[0])
        elif len(filter_values) > 1:
            df = df.filter(polars.col(filter_column).is_in(filter_values))
        df = df.collect()
        return df
