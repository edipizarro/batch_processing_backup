import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cos, radians, when, abs, struct, explode, collect_list, array, sqrt, log10, lit, count, mean, stddev_pop, sum, max, min
from pyspark import StorageLevel

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, IntegerType, ShortType
spark = SparkSession.builder.config("spark.driver.host", "localhost").config("spark.executor.cores", "6").config("spark.executor.instances", "6").config("spark.sql.streaming.checkpointLocation", "checkpoint/dir").config("spark.driver.memory", "16g").config("spark.executor.memory", "16g").appName("Sparktestspp").master("local[6]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 300)
conf = pyspark.SparkConf()

_THRESHOLD_ZTF = 13.2

# Load all parquets from a folder and its nested ones. Used to load light curve dataframes

def separate_dataframe_corr_df(correction_df):
    detections = correction_df.select(explode("detections").alias("exploded_data")).select("exploded_data.*")
    non_detections = correction_df.select(explode("non_detections").alias("exploded_data")).select("exploded_data.*")
    return detections, non_detections

def separate_dataframe_cobj(correction_df):
    detections = correction_df.select(explode("detections").alias("exploded_data")).select("exploded_data.*")
    return detections

#! Function to create the "object statistic" will choose only the detections non forced
#Functions to create index for detections, for now we first drop it, then create it again, because last correction step ran had still index as a column, to compare with pipeline results
def drop_dupes_detection(detections):
    #detections = detections.drop('index') #! This line must be deleted when correction step drops index
    detections = detections.drop_duplicates(["candid", "oid"])
    return detections

def get_non_forced(corrector_detections):
    corrector_detections = corrector_detections.filter(~col("forced"))
    return corrector_detections

### OBJECT STATS

def first_mjd(detections):
    detections = detections.groupBy("oid").agg(F.min("mjd").alias("first_mjd"))
    return detections

def last_mjd(detections):
    detections = detections.groupBy("oid").agg(F.max("mjd").alias("last_mjd"))
    return detections

def deltajd(detections):
    first_mjd_df = first_mjd(detections)
    last_mjd_df = last_mjd(detections)
    joined_df = first_mjd_df.join(last_mjd_df, on="oid")
    joined_df = joined_df.withColumn("deltajd", joined_df["last_mjd"] - joined_df["first_mjd"])
    joined_df = joined_df.sort("oid")
    return joined_df

def calculate_ndets_oid(detections):
    detections = detections.groupBy("oid").agg(count("*").alias("ndet"))
    return detections

def arcsec2dec_era_edec(non_forced):
    non_forced = non_forced.withColumn("e_ra_arcsec", col("e_ra") / 3600.0)
    non_forced = non_forced.withColumn("e_dec_arcsec", col("e_dec") / 3600.0)
    return non_forced

def create_weighted_columns(non_forced):
    non_forced = non_forced.withColumn("weighted_e_ra", 1 / (col("e_ra_arcsec") ** 2))
    non_forced = non_forced.withColumn("weighted_e_dec", 1 / (col("e_dec_arcsec") ** 2))
    return non_forced

def correct_coordinates(non_forced):
    non_forced = arcsec2dec_era_edec(non_forced)
    non_forced = create_weighted_columns(non_forced)
    weighted_ras = non_forced.groupBy("oid").agg(sum(col("ra") * col("weighted_e_ra")).alias("weighted_sum_ra"),
    sum("weighted_e_ra").alias("total_weight_e_ra")
    )
    weighted_decs = non_forced.groupBy("oid").agg(sum(col("dec") * col("weighted_e_dec")).alias("weighted_sum_dec"),
    sum("weighted_e_dec").alias("total_weight_e_dec")
    )
    weighted_ras = weighted_ras.withColumn("meanra", col("weighted_sum_ra") / col("total_weight_e_ra"))
    weighted_ras = weighted_ras.withColumn("sigmara", 3600.0*sqrt(1/col("total_weight_e_ra")))
    weighted_decs = weighted_decs.withColumn("meandec", col("weighted_sum_dec") / col("total_weight_e_dec"))
    weighted_decs = weighted_decs.withColumn("sigmadec", 3600.0*sqrt(1/col("total_weight_e_dec")))

    corrected_coords = weighted_ras.join(weighted_decs, on="oid").select("oid", "meanra", "meandec", "sigmara", "sigmadec")
    corrected_coords = corrected_coords.sort('oid')
    return corrected_coords

def select_oid_sid_fid(non_forced):
    return non_forced.select("oid", "sid", "fid").distinct()

def select_corrected_stellar_firstmjd(detections):
    detections = detections.select("oid", "mjd", "stellar", "corrected")
    detections = detections.sort("mjd")
    detections = detections.dropDuplicates(["oid"]).drop("mjd")
    return detections

def calculate_object_stats(detections):
    detections_nonforced = get_non_forced(detections)
    dets_num = calculate_ndets_oid(detections_nonforced)
    mjds = deltajd(detections_nonforced)
    stellar_corrected = select_corrected_stellar_firstmjd(detections_nonforced)
    oid_fid_sid = select_oid_sid_fid(detections_nonforced)
    corrected_coordinates = correct_coordinates(detections_nonforced)
    joined_df = dets_num.join(mjds, on="oid")
    joined_df = joined_df.join(corrected_coordinates, on="oid")
    joined_df = joined_df.join(oid_fid_sid, on="oid")
    joined_df = joined_df.join(stellar_corrected, on='oid')
    return joined_df

### MAGSTATS

def calculate_ndets_magstats(detections):
    detections = detections.groupBy("oid", "sid", "fid").agg(count("*").alias("ndet")).select("oid", "sid", "fid", "ndet")
    return detections

def first_mjd_magstats(detections):
    detections = detections.groupBy("oid", "sid", "fid").agg(F.min("mjd").alias("first_mjd"))
    return detections

def last_mjd_magstats(detections):
    detections = detections.groupBy("oid", "sid", "fid").agg(F.max("mjd").alias("last_mjd"))
    return detections

def calculate_mjds_magstats(detections):
    firstmjd = first_mjd_magstats(detections)
    lastmjd = last_mjd_magstats(detections)
    mjds = firstmjd.join(lastmjd, on=["oid", "sid", "fid"])
    return mjds

def calculate_mags_magstats(detections):
    detections_mags = detections.groupBy("oid", "sid", "fid").agg(
    max("mag").alias("magmax"),
    min("mag").alias("magmin"),
    mean("mag").alias("magmean"),
    stddev_pop("mag").alias("magsigma"))
    median_mag = detections.groupBy('oid', 'fid', 'sid').agg(F.expr('percentile(mag, 0.5)').alias('magmedian'))
    detections_mags = detections_mags.join(median_mag, on=['oid', 'fid', 'sid'])
    return detections_mags

def calculate_first_last_mag(detections):
    mag_first = detections.sort("mjd").dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag")
    mag_first = mag_first.withColumnRenamed("mag", "magfirst")
    mag_last = detections.sort("mjd", ascending=False).dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag")
    mag_last = mag_last.withColumnRenamed("mag", "maglast")
    joined_df = mag_first.join(mag_last, on=["oid", "sid", "fid"])
    return joined_df

def calculate_stellar_corrected_magstats(detections):
    detections = detections.select("oid", "sid", "fid", "mjd", "stellar", "corrected")
    detections = detections.sort("mjd")
    detections = detections.dropDuplicates(["oid", "sid", "fid"]).drop("mjd")
    detections = detections.sort('oid')
    return detections

def calculate_mags_corrected_magstats(detections):
    detections_mags_corr = detections.groupBy("oid", "sid", "fid").agg(
    max("mag_corr").alias("magmax_corr"),
    min("mag_corr").alias("magmin_corr"),
    mean("mag_corr").alias("magmean_corr"),
    stddev_pop("mag_corr").alias("magsigma_corr"))
    median_mag_corr = detections.groupBy('oid', 'fid', 'sid').agg(F.expr('percentile(mag_corr, 0.5)').alias('magmedian_corr'))
    detections_mags_corr = detections_mags_corr.join(median_mag_corr, on=['oid', 'fid', 'sid'])
    return detections_mags_corr


def calculate_first_last_mag_corr(detections):
    mag_first = detections.sort("mjd").dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag_corr")
    mag_first = mag_first.withColumnRenamed("mag_corr", "magfirst_corr")
    mag_last = detections.sort("mjd", ascending=False).dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag_corr")
    mag_last = mag_last.withColumnRenamed("mag_corr", "maglast_corr")
    joined_df = mag_first.join(mag_last, on=["oid", "sid", "fid"])
    return joined_df

def calculate_dubious_magstats(detections):
    # it is necessary to convert to numeric values before summing, otherwise it will throw an error
    detections = detections.withColumn("dubious", when(col("dubious"), 1).otherwise(0))
    detections = detections.groupBy("oid", "fid", "sid") \
           .agg(sum("dubious").alias("ndubious"))
    return detections

def calculate_saturation_rate(detections):
    saturation = detections.withColumn("corrected", when(col("corrected"), 1).otherwise(0))
    saturation = saturation.groupBy("oid", "fid", "sid") \
           .agg(sum("corrected").alias("total_saturation"))
    grouped_df = detections.groupBy("oid", "sid", "fid")
    saturated_counts = (
        grouped_df.agg(
            F.sum(F.when(F.col("mag_corr") < _THRESHOLD_ZTF, 1).otherwise(0)).alias("sat_count")
        )
    )
    joined_df = saturated_counts.join(saturation, ["oid", "sid", "fid"], "inner")
    saturation_rate = (
        joined_df
        .withColumn("saturation_rate", 
                    F.when(F.col("total_saturation") != 0, 
                           F.col("sat_count") / F.col("total_saturation")).otherwise(float('nan')))
        .select("oid", "sid", "fid", "saturation_rate")
    )
    saturation_rate.filter(saturation_rate['oid']=='ZTF17aaajbez')
    return saturation_rate

def calculate_dmdt(detections, non_detections):
    dt_min = 0.5 
    first_mag = detections.sort("mjd").dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag")
    first_e_mag = detections.sort("mjd").dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "e_mag")
    first_mjd = detections.sort("mjd").dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mjd")
    joined_df_detections = first_mag.join(first_e_mag, on=["oid", "sid", "fid"]).join(first_mjd, on=["oid", "sid", "fid"])

    non_detections_dmdt = non_detections.sort("mjd").select("oid", "sid", "fid", "mjd", "diffmaglim")
    non_detections_dmdt = non_detections_dmdt.withColumnRenamed('mjd', 'mjdnd')
    #! making an attempt to keep the order from the pipeline results
    joined_dets_ndets = joined_df_detections.join(non_detections_dmdt, on=["oid", "sid", "fid"])
    joined_dets_ndets = joined_dets_ndets.withColumn("dt_first", joined_dets_ndets["mjd"] - joined_dets_ndets["mjdnd"])
    joined_dets_ndets = joined_dets_ndets.withColumn("dm_first", joined_dets_ndets["mag"] - joined_dets_ndets["diffmaglim"])
    joined_dets_ndets = joined_dets_ndets.withColumn("sigmadm_first", joined_dets_ndets["e_mag"] - joined_dets_ndets["diffmaglim"])
    joined_dets_ndets = joined_dets_ndets.withColumn("dmdt_first", (joined_dets_ndets["mag"] + joined_dets_ndets["e_mag"] - joined_dets_ndets["diffmaglim"]) / joined_dets_ndets["dt_first"])
    joined_dets_ndets_dt_min = joined_dets_ndets.filter(joined_dets_ndets["dt_first"] > dt_min)
    joined_dets_ndets_dt_min = joined_dets_ndets_dt_min.sort('dmdt_first').dropDuplicates(["oid", "sid", "fid"])
    joined_dets_ndets_dt_min = joined_dets_ndets_dt_min.select('oid', 'sid', 'fid', 'dt_first', 'dm_first', 'sigmadm_first', 'dmdt_first')
    """
    #selecting last nd of each oid (meaning, newest non-detection)...
    joined_dets_ndets_dt_min = joined_dets_ndets_dt_min.sort('mjdnd', ascending=False).dropDuplicates(["oid", "sid", "fid"])
    joined_dets_ndets_dt_min.sort('oid').show()
    """
    return joined_dets_ndets_dt_min


def calculate_magstats(detections, non_detections):
    ndets = calculate_ndets_magstats(detections)
    mjds = calculate_mjds_magstats(detections)
    mags = calculate_mags_magstats(detections)
    first_last_mags = calculate_first_last_mag(detections)   
    stellar_corrected = calculate_stellar_corrected_magstats(detections)
    mags_corr = calculate_mags_corrected_magstats(detections)
    first_last_mags_corr = calculate_first_last_mag_corr(detections)
    dubious = calculate_dubious_magstats(detections)
    sat = calculate_saturation_rate(detections)
    dmdt = calculate_dmdt(detections, non_detections)
    magstats = mjds.join(first_last_mags, on=["oid", "sid", "fid"])\
        .join(first_last_mags_corr, on=["oid", "sid", "fid"])\
        .join(dubious, on=["oid", "sid", "fid"])\
        .join(stellar_corrected, on=["oid", "sid", "fid"])\
        .join(ndets, on=["oid", "sid", "fid"])\
        .join(dmdt, on=["oid", "sid", "fid"], how='left')\
        .join(mags_corr, on=["oid", "sid", "fid"])\
        .join(mags, on=["oid", "sid", "fid"])\
        .join(sat, on=["oid", "sid", "fid"], how='left')    
    return magstats

def execute_objstats(correction_df):
    detections = separate_dataframe_cobj(correction_df)
    detections = drop_dupes_detection(detections)
    object_stats = calculate_object_stats(detections)
    return object_stats


def execute_magstats(correction_df):
    detections, non_detections = separate_dataframe_corr_df(correction_df)
    detections = drop_dupes_detection(detections)
    detections_non_forced = get_non_forced(detections)
    magstats = calculate_magstats(detections_non_forced, non_detections)
    return magstats
