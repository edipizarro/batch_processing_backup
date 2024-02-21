from pyspark.sql import SparkSession
from batch_processing import ZTFUtils

spark = (
    SparkSession.builder.appName("ZTFUtils")
    .master("local")
    .config("spark.executor.memory", "4g")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
    .getOrCreate()
)

config_path = "/home/edipizarro/alerce/batch_processing/configs/raw.config.json"
ztf = ZTFUtils(config_path)
ztf.register_spark_session(spark)
ztf.execute()
