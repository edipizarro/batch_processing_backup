from spark_init.pyspark_configs import *

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
    
    weighted_ras_decs = non_forced.groupBy("oid").agg(
        sum(col("ra") * col("weighted_e_ra")).alias("weighted_sum_ra"),
        sum(col("dec") * col("weighted_e_dec")).alias("weighted_sum_dec"),
        sum("weighted_e_ra").alias("total_weight_e_ra"),
        sum("weighted_e_dec").alias("total_weight_e_dec")
    )
    
    corrected_coords = weighted_ras_decs.withColumn("meanra", col("weighted_sum_ra") / col("total_weight_e_ra")) \
                                       .withColumn("sigmara", 3600.0 * sqrt(1 / col("total_weight_e_ra"))) \
                                       .withColumn("meandec", col("weighted_sum_dec") / col("total_weight_e_dec")) \
                                       .withColumn("sigmadec", 3600.0 * sqrt(1 / col("total_weight_e_dec"))) \
                                       .select("oid", "meanra", "meandec", "sigmara", "sigmadec") \
                                       .orderBy("oid")
    
    return corrected_coords

def select_oid_sid_fid(non_forced):
    return non_forced.select("oid", "sid", "fid").distinct()

def select_corrected_stellar_firstmjd(detections):
    detections = detections.select("oid", "stellar", "corrected").orderBy("mjd")
    detections = detections.dropDuplicates(["oid"]).drop("mjd")
    return detections

def calculate_object_stats(detections):
    detections_nonforced = get_non_forced(detections)
    
    dets_num = calculate_ndets_oid(detections_nonforced)
    mjds = deltajd(detections_nonforced)
    stellar_corrected = select_corrected_stellar_firstmjd(detections_nonforced)
    oid_fid_sid = select_oid_sid_fid(detections_nonforced)
    corrected_coordinates = correct_coordinates(detections_nonforced)
    
    joined_df = dets_num.join(mjds, on="oid").join(corrected_coordinates, on="oid") \
                        .join(oid_fid_sid, on="oid").join(stellar_corrected, on="oid")
    
    return joined_df

### MAGSTATS

def calculate_ndets_magstats(detections):
    windowSpec = Window.partitionBy("oid", "sid", "fid")
    ndet = F.count("*").over(windowSpec).alias("ndet")
    result_df = detections.withColumn("ndet", ndet)    
    return result_df

def first_mjd_magstats(detections):
    return detections.select("oid", "sid", "fid", "mjd", "unique_id").groupBy("oid", "sid", "fid").agg(F.min("mjd").alias("first_mjd"))

def last_mjd_magstats(detections):
    return detections.select("oid", "sid", "fid", "mjd", "unique_id").groupBy("oid", "sid", "fid").agg(F.max("mjd").alias("last_mjd"))


def calculate_mjds_magstats(detections):
    firstmjd = first_mjd_magstats(detections)
    lastmjd = last_mjd_magstats(detections)
    return firstmjd.join(lastmjd, on=["unique_id"]).dropDuplicates()

def calculate_mags_magstats(detections):
    detections_mags = detections.groupBy("oid", "sid", "fid").agg(
        F.max("mag").alias("magmax"),
        F.min("mag").alias("magmin"),
        F.mean("mag").alias("magmean"),
        F.stddev_pop("mag").alias("magsigma")
    )
    median_mag = detections.groupBy("oid", "fid", "sid").agg(
        F.expr("percentile_approx(mag, 0.5)").alias("magmedian")
    )
    return detections_mags.join(median_mag, on=["oid", "fid", "sid"])

def calculate_first_last_mag(detections):
    mag_first = detections.orderBy("mjd").dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag").withColumnRenamed("mag", "magfirst")
    mag_last = detections.orderBy("mjd", ascending=False).dropDuplicates(["oid", "sid", "fid"]).select("oid", "sid", "fid", "mag").withColumnRenamed("mag", "maglast")
    return mag_first.join(mag_last, on=["oid", "sid", "fid"])

def calculate_stellar_corrected_magstats(detections):
    detections = detections.select("oid", "sid", "fid", "stellar", "corrected").orderBy("mjd").dropDuplicates(["oid", "sid", "fid"]).drop("mjd").orderBy("oid")
    return detections

def calculate_mags_corrected_magstats(detections):
    detections_mags_corr = detections.groupBy("oid", "sid", "fid").agg(
        F.max("mag_corr").alias("magmax_corr"),
        F.min("mag_corr").alias("magmin_corr"),
        F.mean("mag_corr").alias("magmean_corr"),
        F.stddev_pop("mag_corr").alias("magsigma_corr")
    )
    median_mag_corr = detections.groupBy('oid', 'fid', 'sid').agg(
        F.expr('percentile(mag_corr, 0.5)').alias('magmedian_corr')
    )
    return detections_mags_corr.join(median_mag_corr, on=['oid', 'fid', 'sid'])


def calculate_first_last_mag_corr(detections):
    mag_first = detections.orderBy("mjd").dropDuplicates(["oid", "sid", "fid"]).selectExpr("oid", "sid", "fid", "mag_corr AS magfirst_corr")
    mag_last = detections.orderBy("mjd", ascending=False).dropDuplicates(["oid", "sid", "fid"]).selectExpr("oid", "sid", "fid", "mag_corr AS maglast_corr")
    joined_df = mag_first.join(mag_last, on=["oid", "sid", "fid"])
    return joined_df

def calculate_dubious_magstats(detections):
    detections = detections.withColumn("dubious_numeric", col("dubious").cast("int"))
    detections = detections.groupBy("oid", "fid", "sid").agg(sum("dubious_numeric").alias("ndubious"))
    detections = detections.drop("dubious_numeric")
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
    return saturation_rate

def calculate_dmdt(detections, non_detections):
    dt_min = 0.5
    first_detections = (
        detections.sort("mjd")
        .dropDuplicates(["oid", "sid", "fid"])
        .select("oid", "sid", "fid", "mag", "e_mag", "mjd"))
    non_detections_dmdt = (
        non_detections.sort("mjd")
        .select("oid", "sid", "fid", col("mjd").alias("mjdnd"), "diffmaglim") )
    joined_df = first_detections.join(non_detections_dmdt, on=["oid", "sid", "fid"])
    joined_df = joined_df.withColumn("dt_first", col("mjd") - col("mjdnd"))
    joined_df = joined_df.withColumn("dm_first", col("mag") - col("diffmaglim"))
    joined_df = joined_df.withColumn("sigmadm_first", col("e_mag") - col("diffmaglim"))
    joined_df = joined_df.withColumn("dmdt_first", (col("mag") + col("e_mag") - col("diffmaglim")) / col("dt_first"))
    joined_df = joined_df.filter(col("dt_first") > dt_min)
    joined_df = joined_df.sort("dmdt_first").dropDuplicates(["oid", "sid", "fid"])
    joined_df = joined_df.select("oid", "sid", "fid", "dt_first", "dm_first", "sigmadm_first", "dmdt_first")
    return joined_df
    """
    #selecting last nd of each oid (meaning, newest non-detection)...
    joined_dets_ndets_dt_min = joined_dets_ndets_dt_min.sort('mjdnd', ascending=False).dropDuplicates(["oid", "sid", "fid"])
    joined_dets_ndets_dt_min.sort('oid').show()
    """


def calculate_magstats(detections, non_detections):
    # Calculate various statistics
    #detections = detections.withColumn("id_detection", monotonically_increasing_id())
    magstats = calculate_ndets_magstats(detections)
    return magstats
    mjds = calculate_mjds_magstats(detections)
    mags = calculate_mags_magstats(detections)
    first_last_mags = calculate_first_last_mag(detections) 
    stellar_corrected = calculate_stellar_corrected_magstats(detections)
    mags_corr = calculate_mags_corrected_magstats(detections)
    first_last_mags_corr = calculate_first_last_mag_corr(detections)
    dubious = calculate_dubious_magstats(detections)
    sat = calculate_saturation_rate(detections)
    dmdt = calculate_dmdt(detections, non_detections)
    magstats = mjds.join(ndets, on=["oid", "sid", "fid"])
    return magstats
    magstats = magstats.join(first_last_mags_corr, on=["oid", "sid", "fid"])
    magstats = magstats.join(dubious, on=["oid", "sid", "fid"])
    magstats = magstats.join(stellar_corrected, on=["oid", "sid", "fid"])
    magstats = magstats.join(ndets, on=["oid", "sid", "fid"])
    magstats = magstats.join(mags_corr, on=["oid", "sid", "fid"])
    magstats = magstats.join(mags, on=["oid", "sid", "fid"])
    magstats = magstats.join(sat, on=["oid", "sid", "fid"])
    magstats = magstats.join(dmdt, on=["oid", "sid", "fid"])
    return magstats

    
    
def execute_objstats(correction_df):
    detections = separate_dataframe_cobj(correction_df)
    detections = drop_dupes_detection(detections)
    object_stats = calculate_object_stats(detections)
    return object_stats


def execute_magstats(correction_df):
    detections, non_detections = separate_dataframe_corr_df(correction_df)
    detections, non_detections = separate_dataframe_corr_df(correction_df)
    detections = drop_dupes_detection(detections)
    detections_non_forced = get_non_forced(detections)
    magstats = calculate_magstats(detections_non_forced, non_detections)
    return magstats
