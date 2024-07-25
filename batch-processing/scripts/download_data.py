from batch_processing.parquet_reader import ParquetBatchReader
from performance_timer import PerformanceTimer


with PerformanceTimer("[SETUP] initialize ParquetReader"):
    config_path = "/home/edipizarro/alerce/batch_processing/batch_processing/configs/read.config.json"
    parquet_reader = ParquetBatchReader(config_path)

with PerformanceTimer("[S3] download data"):
    local_folder_path = "/home/edipizarro/alerce/batch_processing/batch_processing/data/parquet/60305/8/"
    s3_folder_path = "parquet/60305/8/"
    parquet_reader.check_and_download_folder(local_folder_path, s3_folder_path)
