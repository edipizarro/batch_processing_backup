from pyspark.sql import SparkSession

from batch_processing.parquet_reader import ParquetBatchReader
from batch_utils.types import ParquetFolder
from performance_timer import PerformanceTimer


with PerformanceTimer("[SETUP] initialize Parquet Reader"):
    config_path = "/home/edipizarro/alerce/batch_processing/batch_processing/configs/read.config.json"
    parquet_reader = ParquetBatchReader(config_path)

with PerformanceTimer("[SETUP] initialize Spark Session"):
    spark = (
        SparkSession.builder.appName("ParquetBatchReader-Lineal")
        .master("local")
        .config("spark.executor.memory", "4g")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )
    parquet_reader.register_spark_session(spark)

with PerformanceTimer("[ParquetReader] execute df generator"):
    folder = ParquetFolder.RawParquet
    iterate_ids = False # If False, iterates by MJD, if True iterates by each parquet.
    df_generator = parquet_reader.get_df_generator(folder, iterate_ids)

    # Loop through all df in df_generator
    generator_is_empty = False
    while not(generator_is_empty):
        try:
            df = next(df_generator)
            print(df.select("objectId").count())
        except StopIteration:
            generator_is_empty = True
            print("All parquets read succesfully")
