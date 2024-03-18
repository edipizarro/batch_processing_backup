import json
from pyspark.sql import SparkSession
from batch_processing.ztf import ZTFCrawler
from performance_timer import PerformanceTimer

with PerformanceTimer("[SETUP] initialize ZTFCrawler"):
    config_path = "/home/edipizarro/alerce/batch_processing/batch_processing/configs/raw.config.json"
    ztf = ZTFCrawler(config_path)

with PerformanceTimer("[SPARK] initialize Spark Session"):
    spark = (
        SparkSession.builder.appName("ZTFCrawler")
        .master("local")
        .config("spark.executor.memory", "32g")
        .config("spark.driver.memory", "32g") # More memory can write bigger parquets
        .config("spark.sql.debug.maxToStringFields", "100") # If removed -> WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by 
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )
    ztf.register_spark_session(spark)

with PerformanceTimer("[ZTFCrawler] execute"):
    ztf.execute()
