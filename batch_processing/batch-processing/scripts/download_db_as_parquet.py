from batch_processing.db_reader import DBParquetWriter
from performance_timer import PerformanceTimer

with PerformanceTimer("[SETUP] initialize DBParquetWriter"):
    config_path = "/home/edipizarro/projects/batch_processing/batch_processing/configs/db.config.json"
    db_reader = DBParquetWriter(config_path)

with PerformanceTimer("[DBParquetWriter] execute"):
    db_reader.execute()
