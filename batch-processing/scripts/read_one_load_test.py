from batch_processing.parquet_reader import ParquetQueryReader
from batch_utils.types import ParquetFolder
from performance_timer import PerformanceTimer

import polars
def get_list_of_random_candids(mjd, sample_size):
    source = f"/home/edipizarro/alerce/batch_processing/batch_processing/data/raw_parquet/{mjd}/0/*.parquet"
    random_rows = polars.scan_parquet(source, rechunk=True).select("candid").collect().sample(sample_size)
    values_to_query = list(list(random_rows)[0])
    return values_to_query

with PerformanceTimer("[SETUP] initialize ParquetReader"):
    config_path = "/home/edipizarro/alerce/batch_processing/batch_processing/configs/read.config.json"
    parquet_reader = ParquetQueryReader(config_path)

for mjd in ["60306"]:
    iterations_to_run = 1000
    samples_per_iteration = 10
    iterations_to_query = [get_list_of_random_candids(mjd,1) for i in range(iterations_to_run)]
    with PerformanceTimer(f"[POLARS] read selected values {mjd}"):
        for values_to_query in iterations_to_query:
            parquet_id = "0"
            # mjd = "60295"
            selected_columns = ["candid", "cutoutScience", "cutoutTemplate", "cutoutDifference"]
            column_to_query = "candid"
            # values_to_query = ["2541429895115010031","2541259593015015055","2541293471115015012","2541272972815010012","2541272972415015041"]
            df = parquet_reader.df_one(
                parquet_id,
                values_to_query,
                column_to_query,
                selected_columns,
                ParquetFolder.RawParquet,
                mjd=mjd
            )
            print(df)
