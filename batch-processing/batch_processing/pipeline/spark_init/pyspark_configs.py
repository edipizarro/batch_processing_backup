
from __future__ import annotations
from abc import ABC, abstractmethod
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, spark_partition_id, isnull, concat, col, cos, radians, when, first, abs, struct, explode, lit, broadcast, collect_list, coalesce, array, sqrt, log10, sum, count, mean, stddev_pop, max, min, monotonically_increasing_id
from pyspark import StorageLevel
from pyspark.sql.window import Window
import time
from pyspark.sql import Row
from ..api.API import AstroideAPI


from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, IntegerType, ShortType

JARS = (
    "batch_processing/pipeline/spark_init/jars/healpix-1.0.jar, batch_processing/pipeline/spark_init/jars/minimal_astroide-2.0.0.jar"
)


spark = SparkSession.builder.config("spark.memory.fraction", "0.85")\
                            .config("spark.jars", JARS)\
                            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")\
                            .config("spark.driver.host", "localhost")\
                            .config("spark.sql.adaptive.enabled", "true")\
                            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")\
                            .config("spark.driver.memory", "9g")\
                            .config("spark.executor.memory", "6g")\
                            .master("local[5]")\
                            .appName("BatchingTesting")\
                            .getOrCreate()
spark.conf.set("spark.default.parallelism", "15")
spark.conf.set("spark.sql.shuffle.partitions", "15")

conf = pyspark.SparkConf()
sc = spark.sparkContext
