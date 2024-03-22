from batch_processing.db_reader import DBParquetReader
from performance_timer import PerformanceTimer

sample_size = 1000000

with PerformanceTimer("[SETUP] initialize DBParquetReader"):
    config_path = "/home/edipizarro/projects/batch_processing/batch_processing/configs/db.config.json"
    db_reader = DBParquetReader(config_path)

with PerformanceTimer("[DBParquetReader] get sample"):
    print("samples: ", sample_size)
    length = db_reader.len()
    sample = db_reader.df_sample(sample_size, "oid")

    oids = [row[0] for row in sample.iter_rows()]

with PerformanceTimer("[DBParquetReader] query with dict"):
    mapped_data = db_reader.mapped_data("oid", "aid", oids)

    for oid in oids:
        found_one = mapped_data[oid]

    print(f"aid for oid {oid}: ", found_one)
