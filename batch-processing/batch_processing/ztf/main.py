import io

import json
import fastavro.schema
import requests
import tarfile
import os
from datetime import datetime
import tarfile

import fastavro
import polars
import glob
import re


from .utils import (
    date_to_mjd,
    str_to_date,
    dates_between_generator,
    configure_logger,
    _init_path,
    _rm_directory_or_file,
    drop_polars_columms,
)

class ZTFCrawler():
    """
    ZTFCrawler class for handling ZTF data processing tasks.
    """
    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        Initialize the ZTFCrawler class.

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


    def get_downloads_mjd_folder(self, mjd) -> str:
        """
        Get the path where downloaded files for a specific MJD will be saved.

        :param mjd: Modified Julian Date.
        :return: Base path of downloads for the specified MJD.
        """
        return os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"]["CompressedAvros"],
            mjd + self.config["UrlSourceFilePostfix"]
        )
    
    def get_uncompressed_mjd_folder(self, mjd) -> str:
        """
        Get the base path for uncompressed files of a specific MJD.

        :param mjd: Modified Julian Date.
        :return: Base path for uncompressed files of the specified MJD.
        """
        return os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"]["UncompressedAvros"],
            mjd
        )

    def get_parquet_mjd_folder(self, mjd):
        """
        Get the path to the Parquet folder for a specific MJD.

        :param mjd: Modified Julian Date.
        :return: Path to the Parquet folder for the specified MJD.
        """
        return os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"]["RawParquet"],
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
        :param case: String representing the case (e.g., "OverwriteData").
        """
        if ((self._exists_directory_or_file(path))
            and self.config[case] in [True, "True", "true", "T", "t"]):
                self.logger.info(f"Deleting existing files at {path}.")
                _rm_directory_or_file(path)

    def _download_file(self, url: str, path: str):
        """
        Download a file from the given URL and save it to the specified path.

        :param url: URL of the file to be downloaded.
        :param path: Path where the downloaded file will be saved.
        """
        self._rm_directory_or_file(path, "OverwriteData")

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
        avros_folder = self.get_uncompressed_mjd_folder(mjd)
        parquet_folder = self.get_parquet_mjd_folder(mjd)
        for data_folder in (avros_folder, parquet_folder):
            if self._exists_directory_or_file(data_folder):
                self.logger.info(f"{data_folder} exists, TAR won't be downloaded")
                return

        download_directory = os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"]["CompressedAvros"]
        )

        _init_path(download_directory)

        ztf_date = date.strftime("%Y%m%d")
        url = os.path.join(
            self.config["UrlSourceBase"],
            self.config["UrlSourceFilePrefix"] + ztf_date + self.config["UrlSourceFilePostfix"]
        )

        download_path = os.path.join(
            download_directory,
            mjd + self.config["UrlSourceFilePostfix"]
        )
        self._download_file(url, download_path)
    
    def _untar_file(self, source: str, destination_folder: str, progress = False):
        """
        Untar a file into the specified destination folder.

        :param source: Path to the tar file to be untarred.
        :param destination_folder: Folder where the content of the tar file will be extracted.
        :param progress: If True, shows progress and extracts by member (slower).
        """
        self._rm_directory_or_file(destination_folder, "OverwriteData")
        file_size = os.stat(source).st_size/(1024**2)
        self.logger.info(f"Opening {source} with size {file_size:.2f} MB")

        if self._exists_directory_or_file(destination_folder):
            self.logger.info(f"{destination_folder} already exists")
            return
        
        _init_path(destination_folder)
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

        self._rm_directory_or_file(source, "DeleteData")
        self.logger.info("Extraction complete!")

    def _untar_ztf_public(self, mjd: str, progress = False):
        """
        Untar ZTF public data for a specific MJD.

        :param mjd: Modified Julian Date.
        :param progress: If True, shows progress and extracts by member (slower).
        """
        parquet_folder = self.get_parquet_mjd_folder(mjd)
        if self._exists_directory_or_file(parquet_folder):
            self.logger.info(f"{parquet_folder} exists, AVROs won't be extracted")
            return

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
            with open(avro_path,'rb') as encoded_avro:
                avro_reader = fastavro.reader(encoded_avro, avro_schema)
                avro = next(avro_reader)
            yield avro
    
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

    def _write_batch_to_parquet(self, df, output_folder: str, name: str):
        """
        Write a batch of dictionaries to a Parquet file.

        :param batch: List of dictionaries.
        :param output_folder: Folder where the Parquet file will be saved.
        :param name: Param used to identify the parquet file.
        """

        # Generate a unique filename based on timestamp or other criteria
        compression = self.config.get("ParquetCompression")
        parquet_filename = f"{name}.{compression}.parquet"

        # Write the DataFrame to a Parquet file
        parquet_path = os.path.join(output_folder, parquet_filename)

        # Write the DataFrame by transforming first to pandas then to parquet => fix issue reading in spark parquets written in polars
        # Tested different compression levels and techniques and found no difference

        """
        df.write_parquet(
            parquet_path,
            compression = compression,
            compression_level = 1
        )
        """
        df.write_parquet(
            parquet_path,
            compression=compression
        )
        
        self.logger.info(f"Parquet file written: {parquet_path}")

    # Auxiliary function to get the parquet mjd from the path using regex. Used to filter by mjd date
    def get_mjd_from_path(self, path):
        match = re.search(r'raw_parquet/(\d+)/', path)
        if match:
            return int(match.group(1))
        else:
            return None
        
    # Filter to keep only the parquets' paths that have same or less MJD than current
    def filter_parquet_paths(self, parquet_paths, current_mjd):
        filtered_paths = [path for path in parquet_paths if self.get_mjd_from_path(path) <= current_mjd]
        return filtered_paths
    
    # Function to apply filter to the parquet paths
    def paths_parquet_past_mjd(self, current_mjd):
        sorting_hat_parquets_paths = os.path.join(
            self.config["DataFolder"],
            self.config["SubDataFolder"]["RawParquet"],
            "**",
            "*.parquet"
        )
        parquet_files_paths = glob.glob(sorting_hat_parquets_paths, recursive=True)
        filtered_paths = self.filter_parquet_paths(parquet_files_paths, current_mjd)
        return filtered_paths
    
    # Function to split an array in chunks. Used to replace aid/oid in the dataframe without loading all parquets
    def split_array(self, big_array, chunk_size):
        return [big_array[i:i+chunk_size] for i in range(0, len(big_array), chunk_size)]

    def _prepare_batch_to_write(self, avros, output_folder, current_batch):
        df = polars.from_dicts(avros)

        # Drop stamps
        stamps_columns =["cutoutScience", "cutoutTemplate", "cutoutDifference", "candid"]
        df = drop_polars_columms(df,stamps_columns)

        self._write_batch_to_parquet(df, output_folder, current_batch)

    def _create_parquet_files(self, avro_generator, batch_size: int, output_folder: str):
        """
        Create Parquet files from an avro generator.

        :param avro_generator: Generator that yields dictionaries.
        :param batch_size: Size of each batch.
        :param output_folder: Folder where the Parquet files will be saved.
        """

        self._rm_directory_or_file(output_folder, "OverwriteData")
        _init_path(output_folder)

        if batch_size == 0:
            batch = list(avro_generator)
            self._prepare_batch_to_write(batch, output_folder, "0")
        elif batch_size > 0:
            try:
                first = next(avro_generator)
            except StopIteration:
                self.logger.info("Current MJD is empty")
                return
            batch = [first]
            avros_in_batch = 1
            current_batch = 0

            for avro_dict in avro_generator:
                batch.append(avro_dict)
                avros_in_batch += 1

                if avros_in_batch == batch_size:
                    self._prepare_batch_to_write(batch, output_folder, current_batch)
                    batch = []
                    avros_in_batch = 0
                    current_batch += 1

            # Write the remaining items as the last batch (if any)
            if batch:
                self._prepare_batch_to_write(batch, output_folder, current_batch)
        else:
            raise ValueError("batch_size must be a number greater or equal to 0")
    
    def _create_ztf_public_parquet(self, mjd: str):
        """
        Create Parquet files from ZTF public data for a specific MJD.

        :param mjd: Modified Julian Date.
        """
        avro_generator = self._get_ztf_public_avro_generator(mjd, avro_schema=None)
        directory = self.get_parquet_mjd_folder(mjd)
        self._create_parquet_files(avro_generator, self.config["BatchSize"], directory)
        
        avros_directory = self.get_uncompressed_mjd_folder(mjd)
        self._rm_directory_or_file(avros_directory, "DeleteData")
    
    def execute(self):
        """
        Execute the ZTF data processing pipeline.
        """
        DateFormat = self.config["DateFormat"]
        StartDate = str_to_date(self.config["StartDate"], DateFormat)
        EndDate = str_to_date(self.config["EndDate"], DateFormat)
        for date in dates_between_generator(StartDate, EndDate):
            mjd = date_to_mjd(date)
            self._download_ztf_public(date, mjd)
            self._untar_ztf_public(mjd)
            self._create_ztf_public_parquet(mjd)
