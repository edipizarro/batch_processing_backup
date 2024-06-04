import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, explode, array, coalesce
from pyspark.sql import functions as F
spark = SparkSession.builder.config("spark.driver.host", "localhost").appName("SparkExample").getOrCreate()
conf = pyspark.SparkConf()
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import lit
import pandas as pd

spark_context = SparkSession.builder.config(conf=conf).getOrCreate()

# Flag used in light_curve step. If it is False, applies filter
# Otherwise remains with the same detections it outputs
SKIP_MJD_FILTER = False


def pre_execute_light_curve(detections, non_detections, forced_photometries):
    # Get unique oids
    oid_df = detections.select("oid").distinct()
    # Union detections and forced_photometries
    all_dets_joined = detections.unionByName(forced_photometries, allowMissingColumns=True)
    # Create a DataFrame that for each oid has list of candids
    candids = detections.groupBy("oid").agg(collect_list("candid").alias("candids"))
    # Create a DataFrame that contains the last mjd for each oid
    last_mjds = detections.groupBy("oid").agg(F.max("mjd").alias("last_mjds"))
    return oid_df, candids, last_mjds, all_dets_joined, non_detections

def pre_produce_light_curve(oid_df, candids, last_mjds, all_dets_joined, non_detections):
    # Drop duplicates from non_detections
    non_detections = non_detections.dropDuplicates(['oid', 'mjd', 'fid'])
    # Apply mjd filter
    if not SKIP_MJD_FILTER:
        join_mjd_dets = all_dets_joined.join(last_mjds, on="oid")
        all_dets_joined = join_mjd_dets.filter(join_mjd_dets["mjd"] <= join_mjd_dets["last_mjds"]).drop('last_mjds')
    # Group detections by oid
    oid_detections_df = all_dets_joined.groupby('oid').agg(collect_list(struct(all_dets_joined.columns)).alias('detections'))
    # Group non-detections by oid
    oid_non_detections_df = non_detections.groupby('oid').agg(collect_list(struct(non_detections.columns)).alias('non_detections'))
    # Join DataFrames
    output_df = oid_detections_df.join(oid_non_detections_df, on='oid', how='left').join(candids, on='oid', how='left')
    return output_df

def execute_light_curve(detections, non_detections, forced_photometries):
    oid_df = detections.select("oid").distinct()
    all_dets_joined = detections.unionByName(forced_photometries, allowMissingColumns=True)
    candids = detections.groupBy("oid").agg(collect_list("candid").alias("candids"))
    last_mjds = detections.groupBy("oid").agg(F.max("mjd").alias("last_mjds"))
    non_detections = non_detections.dropDuplicates(['oid', 'mjd', 'fid'])
    if not SKIP_MJD_FILTER:
        join_mjd_dets = all_dets_joined.join(last_mjds, on="oid")
        all_dets_joined = join_mjd_dets.filter(join_mjd_dets["mjd"] <= join_mjd_dets["last_mjds"]).drop('last_mjds')
    oid_detections_df = all_dets_joined.groupby('oid').agg(collect_list(struct(all_dets_joined.columns)).alias('detections'))
    oid_non_detections_df = non_detections.groupby('oid').agg(collect_list(struct(non_detections.columns)).alias('non_detections'))
    output_df = oid_detections_df.join(oid_non_detections_df, on='oid', how='left').join(candids, on='oid', how='left')
    return output_df