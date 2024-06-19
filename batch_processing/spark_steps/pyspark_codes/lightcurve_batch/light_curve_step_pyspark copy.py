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


# Load all parquets from a folder and its nested ones. Used to create a single dataframe
def load_dataframes(parquet_dir):
    parquetDataFrame = spark.read.format("parquet").option("recursiveFileLookup", "true").load(parquet_dir)
    return parquetDataFrame

# Load all parquets from detections, non-detections and forced_photometries. Each as a single dataframe
def load_each_dataframe(dets_dir, non_dets_dir, forced_phot_dir):
    detections = load_dataframes(dets_dir)
    non_detections = load_dataframes(non_dets_dir)
    forced_photometries = load_dataframes(forced_phot_dir)
    return detections, non_detections, forced_photometries

#! Anything that applies to detections must apply both to detections and forced_photometries dataframes
#! Revisar candids para forced_photoms? Ese paso lo hace o no?
def pre_execute_light_curve(detections, non_detections, forced_photometries):
    oid_df = detections.select("oid").distinct() # Get unique oids
    detections = detections.withColumn("new", lit(True)) # Add a new column to detections dataframe
    forced_photometries = forced_photometries.withColumn("new", lit(True)) # Add a new column to forced_photometries dataframe (dets contains phots originally)
    all_dets_joined = detections.unionByName(forced_photometries, allowMissingColumns=True)
    candids_oid = detections.select("candid", "oid") # Select candids and oids
    candids = candids_oid.groupBy("oid").agg(collect_list("candid").alias("candids")) # Create a dataframe that for each oid has list of candids
    mjd_oid = detections.select("mjd", "oid") # Select mjd and oids. Assuming max mjd is always in an alert (correct?)
    last_mjds = mjd_oid.groupBy("oid").agg(F.max("mjd").alias("last_mjds"))
    all_dets_joined = detections.unionByName(forced_photometries, allowMissingColumns=True)
    return oid_df, candids, last_mjds, all_dets_joined, non_detections

def execute_light_curve(oid_df, candids, last_mjds, all_dets_joined, non_detections):
    ### El execute no afecta porque es traerse todo lo de la database
    return

def pre_produce_light_curve(oid_df, candids, last_mjds, all_dets_joined, non_detections):
    non_detections = non_detections.drop_duplicates(['oid', 'mjd', 'fid'])
    if SKIP_MJD_FILTER == False: # Este filtro no aplicaría a menos que se hayan procesado parquets después de la fecha de la alerta?
        join_mjd_dets = all_dets_joined.join(last_mjds, on="oid")
        all_dets_filtered = join_mjd_dets.filter(join_mjd_dets["mjd"] <= join_mjd_dets["last_mjds"])
        all_dets_joined = all_dets_filtered.drop('last_mjds')    
    all_dets_joined = all_dets_joined.select(sorted(all_dets_joined.columns))
    oid_detections_df = all_dets_joined.groupby('oid').agg(collect_list(struct(all_dets_joined.columns)).alias('detections'))
    oid_non_detections_df = non_detections.groupby('oid').agg(collect_list(struct(non_detections.columns)).alias('non_detections'))
    output_df = oid_detections_df.join(oid_non_detections_df, on='oid', how='left')
    output_df = output_df.join(candids, on='oid', how='left')

    return output_df


