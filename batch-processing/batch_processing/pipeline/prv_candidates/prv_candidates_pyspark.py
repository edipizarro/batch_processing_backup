from ..spark_init.pyspark_configs import *


_ZERO_MAG = 100.0

def modify_alert_fields(df):
    extra_fields_columns = ['aimage', 'aimagerat', 'bimage', 'bimagerat', 'chinr', 'chipsf', 'classtar', 'clrcoeff', 'clrcounc', 'clrmed', 'clrrms', 'decnr', 'diffmaglim', 'distnr', 'distpsnr1', 'distpsnr2', 'distpsnr3', 'drb', 'drbversion', 'dsdiff', 'dsnrms', 'elong', 'exptime', 'field', 'fp_hists', 'fwhm', 'jdendhist', 'jdendref', 'jdstarthist', 'jdstartref', 'magap', 'magapbig', 'magdiff', 'magfromlim', 'maggaia', 'maggaiabright', 'magnr', 'magzpsci', 'magzpscirms', 'magzpsciunc', 'mindtoedge', 'nbad', 'ncovhist', 'ndethist', 'neargaia', 'neargaiabright', 'nframesref', 'nid', 'nmatches', 'nmtchps', 'nneg', 'objectidps1', 'objectidps2', 'objectidps3', 'pdiffimfilename', 'programid', 'programpi', 'prv_candidates', 'ranr', 'rb', 'rbversion', 'rcid', 'rfid', 'scorr', 'seeratio', 'sgmag1', 'sgmag2', 'sgmag3', 'sgscore1', 'sgscore2', 'sgscore3', 'sharpnr', 'sigmagap', 'sigmagapbig', 'sigmagnr', 'simag1', 'simag2', 'simag3', 'sky', 'srmag1', 'srmag2', 'srmag3', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'ssnrms', 'sumrat', 'szmag1', 'szmag2', 'szmag3', 'tblid', 'tooflag', 'xpos', 'ypos', 'zpclrcov', 'zpmed']
    expanded_extra_fields = df.drop('prv_candidates').drop('fp_hists')

    expanded_extra_fields = expanded_extra_fields.withColumn("has_stamp", lit(False))\
                                                 .withColumn("forced", lit(False))\
                                                 .withColumn("parent_candid", lit("None"))
    
    extra_fields_columns.remove('prv_candidates')
    extra_fields_columns.remove('fp_hists')
    expanded_extra_fields = expanded_extra_fields.withColumn("extra_fields", struct([col(c) for c in extra_fields_columns]))
    expanded_extra_fields = expanded_extra_fields.drop(*extra_fields_columns)
    sorted_columns_df = sorted(expanded_extra_fields.columns)
    expanded_extra_fields = expanded_extra_fields.select(*sorted_columns_df)
    return expanded_extra_fields

def expand_fields_adding_alert_data(df):
    unnested_extra_fields = df
    unnested_extra_fields = unnested_extra_fields.select(*['oid', 'aid', 'ra', 'dec', 'candid', 'fp_hists', 'prv_candidates'])
    unnested_extra_fields = unnested_extra_fields.withColumnRenamed('candid', 'candid_alert')\
                                                 .withColumnRenamed('ra', 'ra_alert')\
                                                 .withColumnRenamed('dec', 'dec_alert')\
                                                 .withColumnRenamed('oid', 'oid_alert')\
                                                 .withColumnRenamed('aid', 'aid_alert')
    
    # Here we have the extra fields with the necessary alert fields for the parsing of each type of detection
    # Now we need to get the rest of the extra fields, and then the detections to be parsed with the alert columns
    unnested_extra_fields_with_previous = unnested_extra_fields.drop('fp_hists')
    unnested_extra_fields_with_fphists = unnested_extra_fields.drop('prv_candidates')

    # We get the extra fields with prv_cands and fp_hists, to get the detections, non_detections and forced_phots separated and ready to be parsed
    # First we explode the prv_cands to be able to separate the detections from the nondets, as well as having them expanded
    unnested_extra_fields_with_previous = unnested_extra_fields_with_previous.withColumn("prv_candidates", explode("prv_candidates"))
    unnested_previous = unnested_extra_fields_with_previous.select("*", col("prv_candidates.*")).drop("prv_candidates")
    non_detections = unnested_previous.filter(col('candid').isNull())
    detections = unnested_previous.filter(col('candid').isNotNull())
    
    # Now we expand the fp_hists to get the forced phots in their expanded form
    unnested_extra_fields_with_fphists = unnested_extra_fields_with_fphists.withColumn("fp_hists", explode("fp_hists"))
    fp_hists = unnested_extra_fields_with_fphists.select("*", col("fp_hists.*")).drop("fp_hists")

    # And just to be careful with the schemas, we sort them all
    sorted_columns_detections = sorted(detections.columns)
    detections = detections.select(*sorted_columns_detections)

    sorted_columns_non_detections = sorted(non_detections.columns)
    non_detections = non_detections.select(*sorted_columns_non_detections)

    sorted_columns_fp_hists = sorted(fp_hists.columns)
    fp_hists = fp_hists.select(*sorted_columns_fp_hists)

    #! Revisar bien las columnas de las cosas que se quedan para dets/fphists/ndets

    return detections, non_detections, fp_hists

def rename_columns(df, type_of_detection):
    if type_of_detection == "previous_detection" or type_of_detection=="forced_photometries":
        df = df.withColumnRenamed("magpsf", "mag")\
               .withColumnRenamed("sigmapsf", "e_mag")\
               .withColumnRenamed("oid_alert", "oid")\
               .withColumnRenamed("jd", "mjd")\
               .withColumnRenamed("aid_alert", "aid")
    elif type_of_detection == "non_detection":
        df = df.withColumnRenamed("oid_alert", "oid")\
               .withColumnRenamed("jd", "mjd")\
               .withColumnRenamed("aid_alert", "aid")
    return df


def apply_transformations(df, type_of_detection):
    if type_of_detection == "previous_detection":
        df = df.withColumn("candid",col("candid").cast(StringType())) \
               .withColumn("oid",col("oid").cast(StringType())) 
        df = df.withColumn("tid", lit("ZTF"))\
           .withColumn("sid", lit("ZTF"))\
           .withColumn("unparsed_isdiffpos", col("isdiffpos"))\
           .withColumn("unparsed_jd", col("mjd"))\
           .withColumn("unparsed_fid", col("fid"))\
           .withColumn("fid", when(df["fid"] == 1, "g")
                             .when(df["fid"] == 2, "r")
                             .otherwise("i"))\
           .withColumn("mjd", col("mjd")- 2400000.5)\
           .withColumn("isdiffpos", when(df["isdiffpos"] == "t", 1)
                                   .when(df["isdiffpos"] == "1", 1)
                                   .otherwise(-1))
        df = df.withColumn("e_dec",  when(df["fid"] == "g", 0.06499999761581421)
                                .when(df["fid"] == "r", 0.08500000089406967)
                                .otherwise(0.01))
        df = df.withColumn("e_dec_decimal",  when(df["fid"] == "g", 0.065)
                                .when(df["fid"] == "r", 0.085)
                                .otherwise(0.01))
        
        condition = cos(radians(col("dec"))) != 0
        calculation = when(condition, col("e_dec_decimal") / abs(cos(radians(col("dec"))))).otherwise(float('nan'))
        df = df.withColumn("e_ra", calculation)
        df = df.withColumn("parent_candid", col("candid_alert"))
        df = df.drop('ra_alert', 'dec_alert', 'candid_alert', 'e_dec_decimal')

        import numpy
        from pyspark.sql.types import FloatType
        def format_and_convert(value):
            # Convert the value to numpy float32
            float32_value = numpy.float32(value)
            # Format to string with high precision (24 decimal places)
            formatted_value = format(float32_value, '.24f')
            # Convert the formatted string back to float
            return float(formatted_value)

        # Register the function as a UDF
        format_and_convert_udf = udf(format_and_convert, FloatType())

        # Apply the UDF to the 'e_ra' column
        df = df.withColumn("e_ra", format_and_convert_udf(df["e_ra"]))



    if type_of_detection == 'non_detection':
        df = df.withColumn("oid",col("oid").cast(StringType()))
        df = df.withColumn("tid", lit("ZTF"))\
                .withColumn("sid", lit("ZTF"))\
                .withColumn("unparsed_fid", col("fid"))\
                .withColumn("fid", when(df["fid"] == 1, "g")
                             .when(df["fid"] == 2, "r")
                             .otherwise("i"))\
                .withColumn("unparsed_jd", col("mjd"))\
                .withColumn("mjd", col("mjd")- 2400000.5)\
                .withColumn("parent_candid", col("candid_alert"))\
                .withColumn("diffmaglim", col("diffmaglim"))    


    if type_of_detection == 'forced_photometries':
        df = df.withColumn("tid", lit("ZTF"))\
               .withColumn("sid", lit("ZTF"))\
               .withColumn("unparsed_fid", col("fid"))\
               .withColumn("fid", when(df["fid"] == 1, "g")
                             .when(df["fid"] == 2, "r")
                             .otherwise("i"))\
               .withColumn("unparsed_jd", col("mjd"))\
               .withColumn("mjd", col("mjd")- 2400000.5)\
               .withColumn("ra", col("ra_alert"))\
               .withColumn("dec", col("dec_alert"))\
               .withColumn("parent_candid", col("candid_alert"))\
               .withColumn("e_dec", lit(0))\
               .withColumn("e_ra", lit(0))
        df = df.withColumn("isdiffpos", when(df["forcediffimflux"]>=0, 1)
                                        .otherwise(-1))
        df = df.drop('ra_alert', 'dec_alert', 'candid_alert')
    return df

def parse_extra_fields(df, type_of_detection):
    if type_of_detection=="previous_detection":
        not_extrafields = [
        "aid",
        "oid",
        "tid",
        "sid",
        "pid",
        "candid",
        "parent_candid",
        "mjd",
        "fid",
        "ra",
        "dec",
        "mag",
        "e_mag",
        "isdiffpos",
        "e_ra",
        "e_dec",
        "unparsed_jd",
        "unparsed_fid", 
        "unparsed_isdiffpos"
        ]
    if type_of_detection=="forced_photometries":
        not_extrafields = [
        "aid",
        "oid",
        "tid",
        "sid",
        "pid",
        "candid",
        "parent_candid",
        "mjd",
        "fid",
        "ra",
        "dec",
        "mag",
        "e_mag",
        "isdiffpos",
        "e_ra",
        "e_dec",
        "unparsed_jd",
        "unparsed_fid"
        ]
    if type_of_detection=="non_detection":
        not_extrafields = [
        "oid",
        "aid",
        "tid",
        "sid",
        "mjd",
        "fid",
        "unparsed_jd",
        "unparsed_fid",
        "diffmaglim",
        "parent_candid" #### for joining purposes 
        ]
    df_extrafields = df.drop(*not_extrafields)
    sorted_columns_extra_fields = sorted(df_extrafields.columns)
    df_extrafields = df_extrafields.select(*sorted_columns_extra_fields)
    columns_extra_fields = df_extrafields.columns
    not_extrafields.append("extra_fields") # adding the extra fields to add to the detection dataframe
    df = df.withColumn("extra_fields", struct([col(c) for c in columns_extra_fields])).select(*[not_extrafields])
    sorted_columns_df = sorted(df.columns)
    df = df.select(*sorted_columns_df)
    if type_of_detection=="non_detection":
        df = df.drop("extra_fields")
    return df


def parser_detections_and_fp(df_candidate, type_of_detection):
    df_candidate = rename_columns(df_candidate, type_of_detection)
    df_candidate = apply_transformations(df_candidate, type_of_detection)
    df_candidate = parse_extra_fields(df_candidate, type_of_detection)
    return df_candidate

def parse_message_detections(df_detections):
    df_detections = parser_detections_and_fp(df_detections, "previous_detection")
    df_detections = df_detections.withColumn("has_stamp", lit(False))\
                                .withColumn("forced", lit(False))
    df_detections = df_detections.select(sorted(df_detections.columns))
    return df_detections

def parse_message_non_detections(df_non_detections):
        df_non_detections = parser_detections_and_fp(df_non_detections, "non_detection")
        return df_non_detections

def isclose(a, b, rtol=1e-05, atol=1e-08):
    abs_diff = abs(a - b)
    threshold = atol + rtol * abs(b)
    return abs_diff <= threshold

def calculate_mag(fp_data):
    fp_data = fp_data.withColumn("flux2uJy", F.pow(10, (F.lit(8.9) - F.col("magzpsci")) / F.lit(2.5)) * F.lit(1.0e6))
    fp_data = fp_data.withColumn("modified_forcediffimflux", F.col("forcediffimflux") * F.col("flux2uJy"))
    fp_data = fp_data.withColumn("modified_forcediffimfluxunc", F.col("forcediffimfluxunc") * F.col("flux2uJy"))

    mag = -2.5 * F.log10(F.abs(F.col("modified_forcediffimflux"))) + F.lit(23.9)
    e_mag = 1.0857 * F.col("modified_forcediffimfluxunc") / F.abs(F.col("modified_forcediffimflux"))
    fp_data = fp_data.withColumn("mag", mag).withColumn("e_mag", e_mag)

    _ZERO_MAG_condition_col = lit(-99999)
    mask_forcediffimflux = isclose(F.col("forcediffimflux"),_ZERO_MAG_condition_col)
    mask_forcediffimfluxunc = isclose(F.col("forcediffimfluxunc"),_ZERO_MAG_condition_col)
    
    # Applying masks to set values to _ZERO_MAG
    fp_data = fp_data.withColumn("mag", F.when(mask_forcediffimflux, _ZERO_MAG).otherwise(F.col("mag")))
    fp_data = fp_data.withColumn("e_mag", F.when(mask_forcediffimflux, _ZERO_MAG).otherwise(F.col("e_mag")))
    fp_data = fp_data.withColumn("e_mag", F.when(mask_forcediffimfluxunc, _ZERO_MAG).otherwise(F.col("e_mag")))
    fp_data = fp_data.drop('flux2uJy', 'modified_forcediffimflux', 'modified_forcediffimfluxunc')
    return fp_data

def parse_message_forced_photometry(forced_photometry):
    forced_photometry = forced_photometry.withColumn("candid", F.concat(forced_photometry['oid_alert'], F.lit('_'), forced_photometry['pid']))
    #! DISCARTING THE FORCED PHOTOMETRY FOR WHEN forcediffimflux == -99999 or forcediffimfluxunc == -99999
    forced_photometry = forced_photometry.filter(col('forcediffimflux')!=-99999)
    forced_photometry = forced_photometry.filter(col('forcediffimfluxunc')!=-99999)
    #! IF WE WANT TO KEEP THESE DATA IN THE DATABASE, WE MUST REMOVE THE PREVIOUS TWO LINES.
    #! ANOTHER OPTION IS TO ADD ANOTHER COLUMN WITH A FLAG FOR "NULL" VALUES.

    forced_photometry = calculate_mag(forced_photometry)
    forced_photometry = forced_photometry.withColumn("magpsf",forced_photometry['mag']).drop('mag')
    forced_photometry = forced_photometry.withColumn("sigmapsf", forced_photometry['e_mag']).drop('e_mag')
    parsed_photometry = parser_detections_and_fp(forced_photometry, "forced_photometries")
    parsed_photometry = parsed_photometry.withColumn("has_stamp", F.lit(False)).withColumn("forced", F.lit(True))
    parsed_photometry = parsed_photometry.select(sorted(parsed_photometry.columns))
    return parsed_photometry

def parse_messages(df):
    detections, non_detections, fp_hists = expand_fields_adding_alert_data(df)
    detections_parsed = parse_message_detections(detections)
    non_detections_parsed = parse_message_non_detections(non_detections)
    forced_photometry_parsed = parse_message_forced_photometry(fp_hists)
    return detections_parsed, non_detections_parsed, forced_photometry_parsed


def restruct_detections(df):
    detections_parsed, non_detections_parsed, forced_photometry_parsed = parse_messages(df)
    grouped_dets = detections_parsed.groupBy('parent_candid', 'oid') \
                                    .agg(collect_list(struct(*[col(c) for c in detections_parsed.columns])).alias("detections")) \
                                    .withColumnRenamed('parent_candid', 'candid')
    
    grouped_phots = forced_photometry_parsed.groupBy('parent_candid', 'oid') \
                                    .agg(collect_list(struct(*[col(c) for c in forced_photometry_parsed.columns])).alias("forced_photometries")) \
                                    .withColumnRenamed('parent_candid', 'candid')
    
    grouped_ndets = non_detections_parsed.groupBy('parent_candid', 'oid') \
                                    .agg(collect_list(struct(*[col(c) for c in non_detections_parsed.columns])).alias("non_detections")) \
                                    .withColumnRenamed('parent_candid', 'candid')
    
    return grouped_dets, grouped_phots, grouped_ndets

    

def extract_detections_and_non_detections_dataframe_reparsed(df):
    modified_alert = modify_alert_fields(df)
    grouped_dets, grouped_fphots, grouped_ndets = restruct_detections(df)

    parsed_df = modified_alert.join(grouped_dets, on=['oid', 'candid'], how='left') \
                          .join(grouped_fphots, on=['oid', 'candid'], how='left') \
                          .join(grouped_ndets, on=['oid', 'candid'], how='left')

    parsed_df = parsed_df.select(sorted(parsed_df.columns))
    return parsed_df

