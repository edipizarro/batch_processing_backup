from ..spark_init.pyspark_configs import *

DISTANCE_THRESHOLD = 1.4
SCORE_THRESHOLD = 0.4
CHINR_THRESHOLD = 2
SHARPNR_MAX = 0.1
SHARPNR_MIN = -0.13
_ZERO_MAG = 100.0


def separate_dataframe_lc_df(lightcurve_df):
    detections_df = lightcurve_df.selectExpr("inline(detections)")
    non_detections_df = lightcurve_df.selectExpr("inline(non_detections)")
    candids = lightcurve_df.select("candids", "oid")
    return detections_df, non_detections_df, candids


def init_corrector(detections):
    df2 = detections.repartition("oid")
    corrector_detections = df2.dropDuplicates(subset=["candid", "oid"]).select("oid", "aid", "candid", "dec", "e_dec", "e_mag", "e_ra", "fid", "forced", "has_stamp",
                                                                               "isdiffpos", "mag", "mjd", "parent_candid", "pid", "ra","sid", "tid", "unparsed_fid",
                                                                                "unparsed_isdiffpos", "unparsed_jd", "extra_fields.*")
    return corrector_detections


def is_corrected_func(corrector_detections):
    condition = corrector_detections["distnr"] < DISTANCE_THRESHOLD
    corrector_detections = corrector_detections.withColumn("corrected", when(condition, True).otherwise(False))
    return corrector_detections

def is_first_corrected(corrector_detections):
    #! Corregir uso de la window
    window_spec = Window.partitionBy("oid", "fid").orderBy("mjd")
    corrector_detections = corrector_detections.withColumn("min_mjd", F.min("mjd").over(window_spec))
    corrector_detections = corrector_detections.withColumn("is_first_corrected", 
                                                           F.when(F.col("mjd") == F.min("mjd").over(window_spec), True)
                                                           .otherwise(False))
    corrector_detections = corrector_detections.drop("min_mjd")
    return corrector_detections


def is_dubious_func(corrector_detections):
    corrector_detections = corrector_detections.withColumn("is_negative", when(corrector_detections["isdiffpos"] == -1, True).otherwise(False))
    corrector_detections = is_corrected_func(corrector_detections)
    corrector_detections = is_first_corrected(corrector_detections)

    is_dubious_condition = (
        (~col("corrected") & col("is_negative")) |
        (col("is_first_corrected") & ~col("corrected")) |
        (~col("is_first_corrected") & col("corrected"))
    )
    corrector_detections = corrector_detections.withColumn("dubious", is_dubious_condition)\
                                               .drop("is_negative", "is_first_corrected")
    return corrector_detections

def is_stellar_func(corrector_detections):
    # Define conditions
    condition_near_ps1 = corrector_detections["distpsnr1"] < DISTANCE_THRESHOLD
    condition_stellar_ps1 = corrector_detections["sgscore1"] > SCORE_THRESHOLD
    sharpnr_in_range_condition = (col("sharpnr") > SHARPNR_MIN) & (col("sharpnr") < SHARPNR_MAX)
    chinr_condition = col("chinr") < CHINR_THRESHOLD
    stellar_ztf_condition = chinr_condition & sharpnr_in_range_condition

    # Apply conditions and create necessary columns
    corrector_detections = corrector_detections.withColumn("near_ps1", when(condition_near_ps1, True).otherwise(False))\
                                               .withColumn("stellar_ps1", when(condition_stellar_ps1, True).otherwise(False))\
                                               .withColumn("near_ztf", col("corrected"))\
                                               .withColumn("stellar_ztf", stellar_ztf_condition)

    # Define is_stellar_condition
    condition1 = (col("near_ztf") & col("near_ps1") & col("stellar_ps1"))
    condition2 = (col("near_ztf") & ~col("near_ps1") & col("stellar_ztf"))
    is_stellar_condition = condition1 | condition2

    # Apply is_stellar_condition and drop unnecessary columns
    corrector_detections = corrector_detections.withColumn("stellar", is_stellar_condition)\
                                               .drop("near_ps1", "stellar_ps1", "near_ztf", "stellar_ztf")

    return corrector_detections


# Create a function to apply logic of numpy is close to two dataframes in pyspark
def isclose(a, b, rtol=1e-05, atol=1e-08):
    abs_diff = abs(a - b)
    threshold = atol + rtol * abs(b)
    return when(abs_diff <= threshold, True).otherwise(False)

def infinities_replacer(corrector_detections):
    
    columns_to_select = [col(column_name) for column_name in corrector_detections.columns 
                     if column_name not in ["mag_corr", "e_mag_corr", "e_mag_corr_ext"]]
    
    corrector_detections = corrector_detections.select(
        *columns_to_select,
        when(corrector_detections["mag_corr"] == float('inf'), _ZERO_MAG)
            .when(corrector_detections["mag_corr"] == float('-inf'), None)
            .otherwise(corrector_detections["mag_corr"]).alias("mag_corr"),
        when(corrector_detections["e_mag_corr"] == float('inf'), _ZERO_MAG)
            .when(corrector_detections["e_mag_corr"] == float('-inf'), None)
            .otherwise(corrector_detections["e_mag_corr"]).alias("e_mag_corr"),
        when(corrector_detections["e_mag_corr_ext"] == float('inf'), _ZERO_MAG)
            .when(corrector_detections["e_mag_corr_ext"] == float('-inf'), None)
            .otherwise(corrector_detections["e_mag_corr_ext"]).alias("e_mag_corr_ext")
    )
    return corrector_detections


def not_corrected_func(corrector_detections):
    columns_to_select = [col(column_name) for column_name in corrector_detections.columns 
                     if column_name not in ["mag_corr", "e_mag_corr", "e_mag_corr_ext"]]

    corrector_detections = corrector_detections.select(
        *columns_to_select,
        when(corrector_detections["corrected"] == False, None).otherwise(corrector_detections["mag_corr"]).alias("mag_corr"),
        when(corrector_detections["corrected"] == False, None).otherwise(corrector_detections["e_mag_corr"]).alias("e_mag_corr"),
        when(corrector_detections["corrected"] == False, None).otherwise(corrector_detections["e_mag_corr_ext"]).alias("e_mag_corr_ext"))

    return corrector_detections


def correct(corrector_detections):
    # Calculate auxiliary columns
    corrector_detections = corrector_detections.withColumn("aux1", pow(10, -0.4 * col("magnr").cast("float")))\
                                               .withColumn("aux2", pow(10, -0.4 * col("mag")))
    # Calculate 'aux3' column
    condition_aux3 = col("aux1") + col("isdiffpos") * col("aux2")
    corrector_detections = corrector_detections.withColumn("aux3", when(condition_aux3 >= 0, condition_aux3).otherwise(0.0))
    # Calculate 'mag_corr' column
    corrector_detections = corrector_detections.withColumn("mag_corr", -2.5 * log10(col("aux3")))
    # Calculate 'aux4' column
    corrector_detections = corrector_detections.withColumn("aux4", (col("aux2") * col("e_mag")) ** 2 - (col("aux1") * col("sigmagnr").cast("float")) ** 2)
    # Calculate 'e_mag_corr' and 'e_mag_corr_ext' columns
    condition_aux4 = col("aux4") < 0
    e_mag_col = when(condition_aux4, float("inf")).otherwise(sqrt(col("aux4")) / col("aux3"))
    corrector_detections = corrector_detections.withColumn("e_mag_corr", e_mag_col)\
                                               .withColumn("e_mag_corr_ext", (col("aux2") * col("e_mag")) / col("aux3"))
    # Create DataFrame of zero magnitude
    _ZERO_MAG_col = lit(_ZERO_MAG)
    # Mask condition for 'mag'
    mask_condition_mag = isclose(corrector_detections["mag"], _ZERO_MAG_col)
    # Apply mask for 'mag_corr' and related columns
    corrector_detections = corrector_detections.withColumn("mask", when(mask_condition_mag, True).otherwise(False))\
                                               .withColumn("mag_corr", when(mask_condition_mag, float('inf')).otherwise(corrector_detections["mag_corr"]))\
                                               .withColumn("e_mag_corr", when(mask_condition_mag, float('inf')).otherwise(corrector_detections["e_mag_corr"]))\
                                               .withColumn("e_mag_corr_ext", when(mask_condition_mag, float('inf')).otherwise(corrector_detections["e_mag_corr_ext"]))
    # Mask condition for 'e_mag'
    mask_condition_e_mag = isclose(corrector_detections["e_mag"], _ZERO_MAG_col)
    # Apply mask for 'e_mag_corr' and related columns
    corrector_detections = corrector_detections.withColumn("mask", when(mask_condition_e_mag, True).otherwise(False))\
                                               .withColumn("e_mag_corr", when(mask_condition_e_mag, float('inf')).otherwise(corrector_detections["e_mag_corr"]))\
                                               .withColumn("e_mag_corr_ext", when(mask_condition_e_mag, float('inf')).otherwise(corrector_detections["e_mag_corr_ext"]))
    # Drop intermediate and unnecessary columns
    corrector_detections = corrector_detections.drop('aux1', 'aux2', 'aux3', 'aux4', 'mask')
    # Sort columns
    sorted_columns = sorted(corrector_detections.columns)
    corrector_detections = corrector_detections.select(*sorted_columns)
    # Replace infinity values
    corrector_detections = infinities_replacer(corrector_detections)
    # Additional function call
    corrector_detections = not_corrected_func(corrector_detections)
    return corrector_detections

def restruct_extrafields(corrector_detections):
    columns_not_extrafields = ['aid', 'candid', 'corrected', 'dec', 'dubious', 'e_dec', 'e_mag', 'e_mag_corr', 'e_mag_corr_ext', 'e_ra', 'fid', 'forced', 'has_stamp', 'isdiffpos', 'mag', 'mag_corr', 'mjd', 'oid', 'parent_candid', 'pid', 'ra', 'sid', 'stellar', 'tid', 'unparsed_fid', 'unparsed_isdiffpos', 'unparsed_jd']
    columns_to_nest = [col for col in corrector_detections.columns if col not in columns_not_extrafields]
    nested_col = struct(*columns_to_nest)
    corrector_detections = corrector_detections.select('*', nested_col.alias('extra_fields'))
    corrector_detections = corrector_detections.select('aid', 'candid', 'corrected', 'dec', 'dubious', 'e_dec', 'e_mag', 'e_mag_corr', 'e_mag_corr_ext', 'e_ra', 'extra_fields', 'fid', 'forced', 'has_stamp', 'isdiffpos', 'mag', 'mag_corr', 'mjd', 'oid', 'parent_candid', 'pid', 'ra', 'sid', 'stellar', 'tid', 'unparsed_fid', 'unparsed_isdiffpos', 'unparsed_jd')
    return corrector_detections

# Add a column to the dataframe, corresponding to the transformed e_ra and e_dec to arcsec
def arcsec2dec_era_edec(non_forced):
    non_forced = non_forced.withColumn("e_ra_arcsec", col("e_ra") / 3600.0)\
                           .withColumn("e_dec_arcsec", col("e_dec") / 3600.0)
    return non_forced

def create_weighted_columns(non_forced):
    non_forced = non_forced.withColumn("weighted_e_ra", pow(col("e_ra_arcsec"), -2.0).cast('float'))\
                           .withColumn("weighted_e_dec", pow(col("e_dec_arcsec"), -2.0).cast('float'))
    non_forced.filter(col('oid')=='ZTF22abuviyj').show()
    return non_forced

def get_non_forced(corrector_detections):
    corrector_detections = corrector_detections.filter(~col("forced"))
    return corrector_detections

def correct_coordinates(non_forced):
    # Calculate weighted sum of RA and weighted sum of Dec
    weighted_coords = non_forced.groupBy("oid").agg(
        sum(col("ra") * col("weighted_e_ra")).alias("weighted_sum_ra"),
        sum(col("dec") * col("weighted_e_dec")).alias("weighted_sum_dec"),
        sum("weighted_e_ra").alias("total_weight_e_ra"),
        sum("weighted_e_dec").alias("total_weight_e_dec")
    )

    # Calculate mean RA and mean Dec
    corrected_coords = weighted_coords.withColumn("meanra", col("weighted_sum_ra") / col("total_weight_e_ra"))\
                                      .withColumn("meandec", col("weighted_sum_dec") / col("total_weight_e_dec"))\
                                      .select("oid", "meanra", "meandec")

    return corrected_coords


def execute_corrector(lightcurve_df):
    detections, non_detections, candids = separate_dataframe_lc_df(lightcurve_df)
    print('Separated dataframes')
    corrector_detections = init_corrector(detections)
    print('Separated corrector detections')
    corrector_detections = is_corrected_func(corrector_detections)
    print('Completed is corrected')
    corrector_detections = is_dubious_func(corrector_detections)
    print('Completed is dubious')
    corrector_detections = is_stellar_func(corrector_detections)
    print('Completed is stellar')


    print('Corrected detections:')
    corrector_detections = correct(corrector_detections)
    corrector_detections = restruct_extrafields(corrector_detections)
    non_forced = get_non_forced(corrector_detections)
    print('NON FORCED NUMBER OF ROWS: ', non_forced.count())
    non_forced = arcsec2dec_era_edec(non_forced)
    non_forced = create_weighted_columns(non_forced)
    print('Completed mag corrections:')
    corrected_coordinates = correct_coordinates(non_forced)
    print('Completed coords corrections:')
    non_detections = non_detections.repartition('oid')
    non_detections = non_detections.drop_duplicates(["oid", "mjd", "fid"])
    sorted_columns_nondetections = sorted(non_detections.columns)
    non_detections = non_detections.select(*sorted_columns_nondetections)
    return corrector_detections, corrected_coordinates, non_detections, candids


def produce_correction(lightcurve_df):
    corrector_detections, corrected_coordinates, non_detections, candids = execute_corrector(lightcurve_df)
    print('Preparing output (joining results)...')
    oid_detections_df = corrector_detections.groupBy('oid').agg(collect_list(struct(corrector_detections.columns)).alias('detections'))
    non_detections = non_detections.groupBy('oid').agg(collect_list(struct(non_detections.columns)).alias('non_detections'))
    candids = candids.repartition('oid')
    corrected_coordinates = corrected_coordinates.repartition('oid')
    oid_detections_df = oid_detections_df.repartition('oid')
    non_detections = non_detections.repartition('oid')
    correction_step_output = corrected_coordinates.join(candids, on='oid')
    correction_step_output = correction_step_output.join(oid_detections_df, on='oid')
    correction_step_output = correction_step_output.join(non_detections, on='oid', how='left')
    correction_step_output = correction_step_output.select(
        *[col(column_name) for column_name in correction_step_output.columns if column_name != "non_detections"],
        when(col("non_detections").isNotNull(), col("non_detections")).otherwise(array()).alias("non_detections")
    )
    print('CORRECTION STEP OUTPUT: ', correction_step_output.count())
    return correction_step_output