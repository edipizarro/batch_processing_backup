import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import log10, col, struct, array, explode, pow, sqrt, concat, when, abs, sum
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.config("spark.driver.host", "localhost").config("spark.driver.memory", "5g").appName("SparkExample").getOrCreate()
conf = pyspark.SparkConf()
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import lit

DISTANCE_THRESHOLD = 1.4
SCORE_THRESHOLD = 0.4
CHINR_THRESHOLD = 2
SHARPNR_MAX = 0.1
SHARPNR_MIN = -0.13
_ZERO_MAG = 100.0


spark_context = SparkSession.builder.config(conf=conf).getOrCreate()


# Load all parquets from a folder and its nested ones. Used to load light curve dataframes
def load_dataframes(parquet_dir):
    parquetDataFrame = spark.read.format("parquet").option("recursiveFileLookup", "true").load(parquet_dir)
    return parquetDataFrame

def separate_dataframe_lc_df(lightcurve_df):
    detections = lightcurve_df.select("detections")
    exploded_dets = detections.select(explode("detections").alias("exploded_data"))
    detections_df = exploded_dets.select("exploded_data.*")
    non_detections = lightcurve_df.select("non_detections")
    exploded_ndets = non_detections.select(explode("non_detections").alias("exploded_data"))
    non_detections_df = exploded_ndets.select("exploded_data.*")
    non_detections_df
    candids = lightcurve_df.select("candids", "oid")
    return detections_df, non_detections_df, candids

def init_corrector(detections):
    corrector_detections = detections.drop_duplicates(["candid", "oid"]).drop('extra_fields')
    corrector_detections = corrector_detections.withColumn("candid_str", corrector_detections["candid"].cast("string"))
    corrector_detections = corrector_detections.withColumn("oid_str", corrector_detections["oid"].cast("string"))
    corrector_detections = corrector_detections.withColumn("index", concat(corrector_detections["candid_str"], lit("_"), corrector_detections["oid_str"]))
    corrector_detections = corrector_detections.drop("candid_str", "oid_str")
    
    corrector_extrafields = detections.select("extra_fields", "candid", "oid")
    corrector_extrafields = corrector_extrafields.select("candid", "oid", "extra_fields.*")
    corrector_extrafields = corrector_extrafields.drop_duplicates(["candid", "oid"])
    corrector_extrafields = corrector_extrafields.withColumn("candid_str", corrector_extrafields["candid"].cast("string"))
    corrector_extrafields = corrector_extrafields.withColumn("oid_str", corrector_extrafields["oid"].cast("string"))
    corrector_extrafields = corrector_extrafields.withColumn("index", concat(corrector_extrafields["candid_str"], lit("_"), corrector_extrafields["oid_str"]))
    corrector_extrafields = corrector_extrafields.drop("candid_str", "oid_str")    
    corrector_detections = corrector_detections.join(corrector_extrafields, how="left", on=['candid', 'oid', 'index'])    
    sorted_columns = sorted(corrector_detections.columns)
    corrector_detections = corrector_detections.select(*sorted_columns)
    return corrector_detections

def find_extra_fields(oid, candid, corrector_extrafields):
    found_extrafields = corrector_extrafields.filter((corrector_extrafields["oid"] == oid) & (corrector_extrafields["candid"] == candid))
    corrector_extrafields = corrector_extrafields.subtract(found_extrafields)
    return found_extrafields, corrector_extrafields


def is_corrected_func(corrector_detections):
    condition = corrector_detections["distnr"] < DISTANCE_THRESHOLD
    corrector_detections = corrector_detections.withColumn("corrected", when(condition, True).otherwise(False))
    return corrector_detections

def is_first_corrected(corrector_detections):
    window_spec = Window.partitionBy("oid", "fid").orderBy("mjd")
    min_mjd = F.min("mjd").over(window_spec)
    corrector_detections = corrector_detections.withColumn("min_mjd", min_mjd)
    first_corrected = corrector_detections.filter(F.col("mjd") == F.col("min_mjd"))
    result = first_corrected.select("corrected", "oid", "fid")
    result = result.withColumnRenamed("corrected", "is_first_corrected")
    corrector_detections= corrector_detections.join(result,how="left", on=["oid", "fid"])
    corrector_detections = corrector_detections.drop("min_mjd")
    corrector_detections = corrector_detections.distinct()
    return corrector_detections

def is_dubious_func(corrector_detections):
    corrector_detections = corrector_detections.withColumn("is_negative", when(corrector_detections["isdiffpos"] == -1, True).otherwise(False))                                                                                                                                       
    corrector_detections = is_corrected_func(corrector_detections)
    corrector_detections = is_first_corrected(corrector_detections)
    is_dubious_condition = (
    (~F.col("corrected") & F.col("is_negative")) |
    (F.col("is_first_corrected") & ~F.col("corrected")) |
    (~F.col("is_first_corrected") & F.col("corrected")))
    corrector_detections = corrector_detections.withColumn("dubious", is_dubious_condition)
    corrector_detections = corrector_detections.drop("is_negative", "is_first_corrected")
    return corrector_detections

def is_stellar_func(corrector_detections):
    condition_near_ps1 = corrector_detections["distpsnr1"] < DISTANCE_THRESHOLD
    corrector_detections = corrector_detections.withColumn("near_ps1", when(condition_near_ps1, True).otherwise(False))
    condition_stellar_ps1 = corrector_detections["sgscore1"] > SCORE_THRESHOLD
    corrector_detections = corrector_detections.withColumn("stellar_ps1", when(condition_stellar_ps1, True).otherwise(False))
    corrector_detections = is_corrected_func(corrector_detections)
    corrector_detections = corrector_detections.select('*', corrector_detections['corrected'].alias('near_ztf'))
    sharpnr_in_range_condition = (col("sharpnr") > SHARPNR_MIN) & (col("sharpnr") < SHARPNR_MAX)
    chinr_condition = col("chinr") < CHINR_THRESHOLD
    stellar_ztf_condition = chinr_condition & sharpnr_in_range_condition
    corrector_detections = corrector_detections.withColumn("stellar_ztf", stellar_ztf_condition)
    # Apply the is stellar condition
    condition1 = (F.col("near_ztf") & F.col("near_ps1") & F.col("stellar_ps1"))
    condition2 = (F.col("near_ztf") & ~F.col("near_ps1") & F.col("stellar_ztf"))
    is_stellar_condition = condition1 | condition2
    corrector_detections = corrector_detections.withColumn("stellar", is_stellar_condition)
    corrector_detections = corrector_detections.drop('near_ps1', 'stellar_ps1', 'near_ztf', 'stellar_ztf')
    return corrector_detections



# Create a function to apply logic of numpy is close to two dataframes in pyspark
def isclose(a, b, rtol=1e-05, atol=1e-08):
    abs_diff = abs(a - b)
    threshold = atol + rtol * abs(b)
    return abs_diff <= threshold


def infinities_replacer(corrector_detections):
    corrector_detections = corrector_detections.withColumn("mag_corr", 
                                                       when(corrector_detections["mag_corr"] == float('inf'), _ZERO_MAG)
                                                       .otherwise(corrector_detections["mag_corr"]))
    corrector_detections = corrector_detections.withColumn("mag_corr", 
                                                       when(corrector_detections["mag_corr"] == float('-inf'), None)
                                                       .otherwise(corrector_detections["mag_corr"]))
    corrector_detections = corrector_detections.withColumn("e_mag_corr", 
                                                       when(corrector_detections["e_mag_corr"] == float('inf'), _ZERO_MAG)
                                                       .otherwise(corrector_detections["e_mag_corr"]))
    corrector_detections = corrector_detections.withColumn("e_mag_corr", 
                                                       when(corrector_detections["e_mag_corr"] == float('-inf'), None)
                                                       .otherwise(corrector_detections["e_mag_corr"]))
    corrector_detections = corrector_detections.withColumn("e_mag_corr_ext", 
                                                       when(corrector_detections["e_mag_corr_ext"] == float('inf'), _ZERO_MAG)
                                                       .otherwise(corrector_detections["e_mag_corr_ext"]))
    corrector_detections = corrector_detections.withColumn("e_mag_corr_ext", 
                                                       when(corrector_detections["e_mag_corr_ext"] == float('-inf'), None)
                                                       .otherwise(corrector_detections["e_mag_corr_ext"]))
    return corrector_detections


def not_corrected_func(corrector_detections):
    corrector_detections = corrector_detections.withColumn("mag_corr", 
                                                       when(corrector_detections["corrected"] == False, None)
                                                       .otherwise(corrector_detections["mag_corr"]))
    corrector_detections = corrector_detections.withColumn("e_mag_corr", 
                                                       when(corrector_detections["corrected"] == False, None)
                                                       .otherwise(corrector_detections["e_mag_corr"]))
    corrector_detections = corrector_detections.withColumn("e_mag_corr_ext", 
                                                       when(corrector_detections["corrected"] == False, None)
                                                       .otherwise(corrector_detections["e_mag_corr_ext"]))   
 
    return corrector_detections

def correct(corrector_detections):
    corrector_detections = corrector_detections.withColumn("aux1", pow(10, -0.4 * col("magnr").cast("float")))
    corrector_detections = corrector_detections.withColumn("aux2", pow(10, -0.4 * col("mag")))
    condition = (col("aux1") + col("isdiffpos") * col("aux2"))
    corrector_detections = corrector_detections.withColumn("aux3", when(condition >= 0, condition).otherwise(0.0))
    corrector_detections = corrector_detections.withColumn("mag_corr", -2.5 * log10("aux3"))
    corrector_detections =  corrector_detections.withColumn("aux4", (col("aux2") * col("e_mag")) ** 2 - (col("aux1") * col("sigmagnr").cast("float")) ** 2)
    condition = col("aux4") < 0
    e_mag_col = when(condition, float("inf")).otherwise(sqrt(col("aux4")) / col("aux3"))
    corrector_detections = corrector_detections.withColumn("e_mag_corr", e_mag_col)
    e_mag_corr = corrector_detections.select('e_mag_corr')
    corrector_detections = corrector_detections.withColumn("e_mag_corr_ext", 
                                (col("aux2") * col("e_mag") / col("aux3")).alias("e_mag_corr_ext"))
    
    #Create a dataframe of zero magnitude to compare using isclose
    _ZERO_MAG_col = lit(_ZERO_MAG)
    mask_condition_mag = isclose(corrector_detections["mag"], _ZERO_MAG_col)
    corrector_detections = corrector_detections.withColumn("mask", when(mask_condition_mag, True).otherwise(False))
    corrector_detections = corrector_detections.withColumn("mag_corr", when(corrector_detections["mask"], float('inf')).otherwise(corrector_detections["mag_corr"]))\
                     .withColumn("e_mag_corr", when(corrector_detections["mask"], float('inf')).otherwise(corrector_detections["e_mag_corr"]))\
                     .withColumn("e_mag_corr_ext", when(corrector_detections["mask"], float('inf')).otherwise(corrector_detections["e_mag_corr_ext"]))

    mask_condition_e_mag = isclose(corrector_detections["e_mag"], _ZERO_MAG_col)
    corrector_detections = corrector_detections.withColumn("mask", when(mask_condition_e_mag, True).otherwise(False))
    corrector_detections = corrector_detections.withColumn("e_mag_corr", when(corrector_detections["mask"], float('inf')).otherwise(corrector_detections["e_mag_corr"]))\
                     .withColumn("e_mag_corr_ext", when(corrector_detections["mask"], float('inf')).otherwise(corrector_detections["e_mag_corr_ext"]))    
    corrector_detections = corrector_detections.drop('mask').drop('aux1').drop('aux2').drop('aux3').drop('aux4')
    # sort columns before returning
    sorted_columns = sorted(corrector_detections.columns)
    corrector_detections = corrector_detections.select(*sorted_columns)
    # replace infinity values (positive by zeromag and negative by none) in the three corr columns
    corrector_detections = infinities_replacer(corrector_detections)
    corrector_detections = not_corrected_func(corrector_detections)
    return corrector_detections

#! Why drop the columns.self._EXTRA_FIELDS???????
def restruct_extrafields(corrector_detections):
    #corrector_detections = corrector_detections.drop('index') 
    columns_not_extrafields =['aid', 'candid', 'corrected', 'dec', 'dubious', 'e_dec', 'e_mag', 'e_mag_corr', 'e_mag_corr_ext', 'e_ra', 'fid', 'forced', 'has_stamp', 'index', 'isdiffpos', 'mag', 'mag_corr', 'mjd', 'new', 'oid', 'parent_candid', 'pid', 'ra', 'sid', 'stellar', 'tid', 'unparsed_fid', 'unparsed_isdiffpos', 'unparsed_jd']
    columns_to_nest = [col for col in corrector_detections.columns if col not in columns_not_extrafields]
    nested_col = struct(*columns_to_nest)
    corrector_detections = corrector_detections.withColumn('extra_fields', nested_col)
    corrector_detections = corrector_detections.select('aid', 'candid', 'corrected', 'dec', 'dubious', 'e_dec', 'e_mag', 'e_mag_corr', 'e_mag_corr_ext', 'e_ra', 'extra_fields', 'fid', 'forced', 'has_stamp', 'index', 'isdiffpos', 'mag', 'mag_corr', 'mjd', 'new', 'oid', 'parent_candid', 'pid', 'ra', 'sid', 'stellar', 'tid', 'unparsed_fid', 'unparsed_isdiffpos', 'unparsed_jd')
    return corrector_detections

# Add a column to the dataframe, corresponding to the transformed e_ra and e_dec to arcsec
def arcsec2dec_era_edec(non_forced):
    non_forced = non_forced.withColumn("e_ra_arcsec", col("e_ra") / 3600.0)
    non_forced = non_forced.withColumn("e_dec_arcsec", col("e_dec") / 3600.0)
    return non_forced

def create_weighted_columns(non_forced):
    non_forced = non_forced.withColumn("weighted_e_ra", 1 / (col("e_ra_arcsec") ** 2))
    non_forced = non_forced.withColumn("weighted_e_dec", 1 / (col("e_dec_arcsec") ** 2))
    return non_forced

def get_non_forced(corrector_detections):
    corrector_detections = corrector_detections.filter(~col("forced"))
    return corrector_detections

def correct_coordinates(non_forced):
    weighted_ras = non_forced.groupBy("oid").agg(sum(col("ra") * col("weighted_e_ra")).alias("weighted_sum_ra"),
    sum("weighted_e_ra").alias("total_weight_e_ra")
    )
    weighted_decs = non_forced.groupBy("oid").agg(sum(col("dec") * col("weighted_e_dec")).alias("weighted_sum_dec"),
    sum("weighted_e_dec").alias("total_weight_e_dec")
    )
    weighted_ras = weighted_ras.withColumn("meanra", col("weighted_sum_ra") / col("total_weight_e_ra"))
    weighted_decs = weighted_decs.withColumn("meandec", col("weighted_sum_dec") / col("total_weight_e_dec"))
    corrected_coords = weighted_ras.join(weighted_decs, on="oid").select("oid", "meanra", "meandec")
    return corrected_coords


def execute_corrector(lightcurve_df):
    detections, non_detections, candids = separate_dataframe_lc_df(lightcurve_df)
    corrector_detections = init_corrector(detections)
    corrector_detections = is_corrected_func(corrector_detections)
    corrector_detections = is_dubious_func(corrector_detections)
    corrector_detections = is_stellar_func(corrector_detections)

    print('Corrected detections:')
    corrector_detections = correct(corrector_detections)
    corrector_detections = restruct_extrafields(corrector_detections)
    non_forced = get_non_forced(corrector_detections)
    non_forced = arcsec2dec_era_edec(non_forced)
    non_forced = create_weighted_columns(non_forced)
    print('corrected nonforced:')
    print('corrected coords:')
    corrected_coordinates = correct_coordinates(non_forced)

    print('non detections')
    non_detections = non_detections.drop_duplicates(["oid", "mjd", "fid"])
    sorted_columns_nondetections = sorted(non_detections.columns)
    non_detections = non_detections.select(*sorted_columns_nondetections)
    return corrector_detections, corrected_coordinates, non_detections, candids



def produce_correction(lightcurve_df):
    corrector_detections, corrected_coordinates, non_detections, candids= execute_corrector(lightcurve_df)    
    corrector_detections.show(1)
    #! DROP INDEX. ITS NOT USED ANYWHERE ELSE. WE'VE KEPT IT FOR COMPARISON WITH PIPELINE!
    #corrector_detections = corrector_detections.drop("index")
    print('oid detections df')
    oid_detections_df = corrector_detections.groupby('oid').agg(collect_list(struct(corrector_detections.columns)).alias('detections'))
    print('non detections')
    non_detections = non_detections.groupby('oid').agg(collect_list(struct(non_detections.columns)).alias('non_detections'))
    non_detections.show(1)
    print('correction step output')
    correction_step_output = corrected_coordinates.join(candids, on='oid')
    correction_step_output = correction_step_output.join(oid_detections_df, on='oid')
    correction_step_output = correction_step_output.join(non_detections, on='oid', how='left')
    correction_step_output = correction_step_output.withColumn("non_detections", when(col("non_detections").isNotNull(), col("non_detections")).otherwise(array()))
    return correction_step_output



#! Revisar que esta mata tanto la memoria => is_dubious o is_stellar!!! (hay mil warning de memoria)  exceptions
