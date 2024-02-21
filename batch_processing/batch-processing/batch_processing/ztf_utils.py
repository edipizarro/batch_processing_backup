import json
import requests
import tarfile
import os
import shutil
from datetime import datetime

import fastavro
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from .utils import Utils

class ZTFUtils():
    """
    ZTFUtils class for handling ZTF data processing tasks.
    """
    def __init__(self, config_path: str):
        """
        Initialize the ZTFUtils class.

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

    
    def get_downloads_base_folder(self) -> str:
        """
        Get the base path where downloaded files will be saved.

        :return: Base path of downloads.
        """
        return os.path.join(
            self.config["files_base_folder"],
            self.config["downloads_folder"]
        )

    def get_downloads_mjd_folder(self, mjd) -> str:
        """
        Get the path where downloaded files for a specific MJD will be saved.

        :param mjd: Modified Julian Date.
        :return: Base path of downloads for the specified MJD.
        """
        return os.path.join(
            self.config["files_base_folder"],
            self.config["downloads_folder"],
            mjd + self.config["url_source_file_postfix"]
        )
    
    def get_uncompressed_mjd_folder(self, mjd) -> str:
        """
        Get the base path for uncompressed files of a specific MJD.

        :param mjd: Modified Julian Date.
        :return: Base path for uncompressed files of the specified MJD.
        """
        return os.path.join(
            self.config["files_base_folder"],
            self.config["untar_folder"],
            mjd
        )

    def get_parquet_mjd_folder(self, mjd):
        """
        Get the path to the Parquet folder for a specific MJD.

        :param mjd: Modified Julian Date.
        :return: Path to the Parquet folder for the specified MJD.
        """
        return os.path.join(
            self.config["files_base_folder"],
            self.config["parquet_folder"],
            mjd
        )

    def _exists_directory_or_file(self, path: str):
        """
        Check if a directory or file exists.

        :param path: Path to the directory or file.
        """
        return (os.path.isfile(path) or os.path.isdir(path))

    def _rm_directory_or_file(self, path: str, case: str):
        """
        Remove a directory or file based on the specified case.

        :param path: Path to the directory or file.
        :param case: String representing the case (e.g., "overwrite_data").
        """
        if ((self._exists_directory_or_file(path))
            and self.config[case] in [True, "True", "true", "T", "t"]):
                self.logger.info(f"Deleting existing files at {path}.")
                Utils._rm_directory_or_file(path)

    def _download_file(self, url: str, path: str):
        """
        Download a file from the given URL and save it to the specified path.

        :param url: URL of the file to be downloaded.
        :param path: Path where the downloaded file will be saved.
        """
        self._rm_directory_or_file(path, "overwrite_data")

        if self._exists_directory_or_file(path) :
            self.logger.info(f"{path} already exists")
            return

        with requests.get(url, stream=True) as response:
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024 * 10
            downloaded_size = 0
            previous_progress = 0
            
            self.logger.info(f"Starting to download {url}")
            progress = (downloaded_size / total_size) * 100
            
            with open(path, mode="wb") as file:
                for chunk in response.iter_content(chunk_size=block_size):
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    progress = (downloaded_size / total_size) * 100

                    # Log progress only when it reaches every 5%
                    if progress - previous_progress >= 5:
                        self.logger.info(f"Downloading: {progress:.2f}% complete")
                        previous_progress = progress

            self.logger.info("Download complete!")

    def _download_ztf_public(self, date: datetime, mjd: str):
        """
        Download ZTF public data for a specific date and MJD.

        :param date: Date for which ZTF public data is requested.
        :param mjd: Modified Julian Date.
        """
        ztf_date = date.strftime("%Y%m%d")
        url = os.path.join(
            self.config["url_source_base"],
            self.config["url_source_file_prefix"] + ztf_date + self.config["url_source_file_postfix"]
        )
        directory = self.get_downloads_base_folder()
        Utils._init_path(directory)
        path = os.path.join(directory, mjd + self.config["url_source_file_postfix"])
        self._download_file(url, path)
    
    def _untar_file(self, source: str, destination_folder: str, progress = False):
        """
        Untar a file into the specified destination folder.

        :param source: Path to the tar file to be untarred.
        :param destination_folder: Folder where the content of the tar file will be extracted.
        :param progress: If True, shows progress and extracts by member (slower).
        """
        self._rm_directory_or_file(destination_folder, "overwrite_data")
        file_size = os.stat(source).st_size/(1024**2)
        self.logger.info(f"Opening {source} with size {file_size:.2f} MB")

        if self._exists_directory_or_file(destination_folder):
            self.logger.info(f"{destination_folder} already exists")
            return
        
        Utils._init_path(destination_folder)
        with tarfile.open(source, 'r') as tar:
            if not progress:
                tar.extractall(destination_folder, filter="data")
                self.logger.info("Extraction started")
            elif progress:
                total_members = len(tar.getmembers())
                extracted_members = 0

                for member in tar.getmembers():
                    tar.extract(member, destination_folder, filter="data")
                    extracted_members += 1

                    progress = (extracted_members / total_members) * 100
                    self.logger.info(f"Extraction Progress: {progress:.2f}% | Currently extracting: {member.name}")

        self._rm_directory_or_file(source, "delete_data")
        self.logger.info("Extraction complete!")

    def _untar_ztf_public(self, mjd: str, progress = False):
        """
        Untar ZTF public data for a specific MJD.

        :param mjd: Modified Julian Date.
        :param progress: If True, shows progress and extracts by member (slower).
        """
        source = self.get_downloads_mjd_folder(mjd)
        destination_folder = self.get_uncompressed_mjd_folder(mjd)
        self._untar_file(source, destination_folder, progress)

    def _get_avro_generator(self, avro_paths: list[str], avro_schema=None):
        """
        Generate Avro data from a list of Avro file paths.

        :param avro_paths: List of Avro file paths.
        :param avro_schema: Avro schema for data reading.
        :return: Avro data generator.
        """
        for avro_path in avro_paths:
            with open(avro_path,'rb') as avro_file:
                freader = fastavro.reader(avro_file, avro_schema)
                packet = next(freader)
            yield packet
    
    def _get_ztf_public_avro_generator(self, mjd: str, avro_schema=None):
        """
        Get Avro data generator for ZTF public data for a specific MJD.

        :param mjd: Modified Julian Date.
        :param avro_schema: Avro schema for data reading.
        :return: Avro data generator.
        """
        directory = self.get_uncompressed_mjd_folder(mjd)
        avro_paths = [os.path.join(directory, avro) for avro in os.listdir(directory)]
        return self._get_avro_generator(avro_paths, avro_schema)

    def _infer_schema(self, example_dict: dict) -> StructType:
        """
        Infer the schema of a PySpark DataFrame from an example dictionary.

        :param example_dict (dict): An example dictionary.
        :return StructType: The inferred PySpark StructType schema.
        """
        fields = [StructField(key, StringType(), nullable=True) for key in example_dict.keys()]
        return StructType(fields)

    def _write_batch_to_parquet(self, batch: list[dict], schema: StructType, output_folder: str, batch_id: int):
        """
        Write a batch of dictionaries to a Parquet file using PySpark.

        :param batch: List of dictionaries.
        :param schema: PySpark StructType schema.
        :param output_folder: Folder where the Parquet file will be saved.
        :param batch_id: Param used to identify the parquet file.
        """
        # Convert the batch to a PySpark DataFrame
        df = self.spark.createDataFrame(batch, schema)

        # Generate a unique filename based on timestamp or other criteria
        parquet_filename = f"{batch_id}"

        # Write the PySpark DataFrame to a Parquet file
        parquet_path = os.path.join(output_folder, parquet_filename)
        df.write.parquet(parquet_path, mode="ignore")

        self.logger.info(f"Parquet file written: {parquet_path}")

    def _create_parquet_files(self, avro_generator, batch_size: int, output_folder: str):
        """
        Create Parquet files from an avro generator using PySpark.

        :param avro_generator: Generator that yields dictionaries.
        :param batch_size: Size of each batch.
        :param output_folder: Folder where the Parquet files will be saved.
        """

        self._rm_directory_or_file(output_folder, "overwrite_data")
        Utils._init_path(output_folder)

        try:
            first = next(avro_generator)
        except StopIteration:
            self.logger.info("Current MJD is empty")
            return
        schema = self._infer_schema(first)

        batch = [first]
        avros_in_batch = 1
        current_batch = 0

        for avro_dict in avro_generator:
            batch.append(avro_dict)
            avros_in_batch += 1

            if avros_in_batch == batch_size:
                self._write_batch_to_parquet(batch, schema, output_folder, current_batch)
                batch = []
                avros_in_batch = 0
                current_batch += 1

        # Write the remaining items as the last batch (if any)
        if batch:
            self._write_batch_to_parquet(batch, schema, output_folder, current_batch)
    
    def _create_ztf_public_parquet(self, mjd: str):
        """
        Create Parquet files from ZTF public data for a specific MJD.

        :param mjd: Modified Julian Date.
        """
        avro_generator = self._get_ztf_public_avro_generator(mjd, avro_schema=None)
        directory = self.get_parquet_mjd_folder(mjd)
        self._create_parquet_files(avro_generator, self.config["batch_size"], directory)
        
        avros_directory = self.get_uncompressed_mjd_folder(mjd)
        self._rm_directory_or_file(avros_directory, "delete_data")
    
    def execute(self):
        """
        Execute the ZTF data processing pipeline.
        """
        date_format = self.config["date_format"]
        start_date = Utils.str_to_date(self.config["start_date"], date_format)
        end_date = Utils.str_to_date(self.config["end_date"], date_format)
        for date in Utils.dates_between_generator(start_date, end_date):
            mjd = Utils.date_to_mjd(date)
            self._download_ztf_public(date, mjd)
            self._untar_ztf_public(mjd)
            self._create_ztf_public_parquet(mjd)
