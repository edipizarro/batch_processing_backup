from pyspark.sql import SparkSession

from batch_processing import ParquetReader

# Create Spark session
spark = (
    SparkSession.builder.appName("ZTFUtils")
    .master("local")
    .config("spark.executor.memory", "4g")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
    .getOrCreate()
)

# Initialize ParquetReader
config_path = "/home/edipizarro/alerce/batch_processing/configs/read.config.json"
parquet_reader = ParquetReader(config_path)
parquet_reader.register_spark_session(spark)

# Start df_generator
df_generator = parquet_reader.get_df_generator()

# Loop through all df in df_generator
generator_is_empty = False
while not(generator_is_empty):
    try:
        df = next(df_generator)
        print(df.select("objectId").count())
    except StopIteration:
        generator_is_empty = True
        print("All parquets read succesfully")
