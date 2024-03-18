from pyspark.sql import SparkSession

from batch_processing.parquet_reader import ParquetBatchReader
from batch_utils.types import ParquetFolder
from performance_timer import PerformanceTimer

# Create Spark session

with PerformanceTimer("[SETUP] initialize Parquet Reader"):
    config_path = "/home/edipizarro/alerce/batch_processing/batch_processing/configs/read.config.json"
    parquet_reader = ParquetBatchReader(config_path)

with PerformanceTimer("[SETUP] initialize Spark Session"):
    spark = (
        SparkSession.builder.appName("ParquetBatchReader-Batch")
        .master("local")
        .config("spark.executor.memory", "4g")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )
    parquet_reader.register_spark_session(spark)

with PerformanceTimer("[ParquetReader] get DF with all data and count rows"):
    df = parquet_reader.df(s3=False, data_folder=ParquetFolder.RawParquet)
    df.show()

    count = df.count()
    print(count)
