from spark_init.pyspark_configs import *
# Create the dataframe of the unique non_detections
def create_non_detections_frame(df):
    non_detections_frame = df.select(F.explode("non_detections").alias("exploded_data"))
    non_detections_frame = non_detections_frame.repartition('oid')
    unique_non_detections = non_detections_frame.select("exploded_data.*").dropDuplicates(['oid', 'mjd'])
    return unique_non_detections


def create_forced_photometries_frame(df):
    fp_frame = df.select(F.explode("forced_photometries").alias("exploded_data"))
    unique_fp = fp_frame.select("exploded_data.*").dropDuplicates(['oid', 'candid'])
    print(f'forced photometries count: {unique_fp.count()}')
    return unique_fp


# Now we want to get the detections dataframe. 
def create_detections_frame(df):
    # We'll first order the dataframe by MJD. This is to retain the newest alerts
    df = df.sort('mjd', ascending = False) 
    alert_df = df.drop('detections', 'non_detections', 'forced_photometries')
    alert_df = alert_df.drop_duplicates(['oid', 'candid'])

    # We expand the extra fields
    alert_df = alert_df.select("*", *[col("extra_fields").getField(field).alias(field) for field in alert_df.select("extra_fields.*").columns])
    alert_df = alert_df.drop('extra_fields')

    # Now we create a detections dataframe where we expand the detections and its extra fields. Same process we did with the alerts
    detections_frame = df
    exploded_detections = detections_frame.select(explode(detections_frame.detections).alias("exploded_data"))
    unnested_detections = exploded_detections.select("exploded_data.*")
    unique_detections = unnested_detections.sort('mjd', ascending = False)
    unique_detections = unnested_detections.drop_duplicates(['oid', 'candid'])
    unique_detections = unique_detections.select("*", *[col("extra_fields").getField(field).alias(field) for field in unique_detections.select("extra_fields.*").columns])
    unique_detections = unique_detections.drop('extra_fields')

    # We rename the columns to identify after the join from which dataframe they come from. We rename oid and candid so they have the same name (it can be done without this step, but there were originally problems not doing it)

    unique_detections = unique_detections.selectExpr([f"{col} as {col}_detection" for col in unique_detections.columns])
    unique_detections = unique_detections.withColumnRenamed("oid_detection", "oid")\
                                         .withColumnRenamed("candid_detection", "candid")
    

    alert_df = alert_df.selectExpr([f"{col} as {col}_alert" for col in alert_df.columns])
    alert_df = alert_df.withColumnRenamed("oid_alert", "oid")\
                       .withColumnRenamed("candid_alert", "candid")
    
    # Directly join the alert data with the detections data

    detections_dataframe = alert_df.join(unique_detections, on = ['oid', 'candid'], how = 'full')   

    # We update the alert data with detection data in the cases where it is required
    
    detections_dataframe = detections_dataframe.select(
    col("candid"),
    col("oid"),
    when(col('aid_detection').isNotNull(), col('aid_detection')).otherwise(col('aid_alert')).alias("aid"),
    when(col('dec_detection').isNotNull(), col('dec_detection')).otherwise(col('dec_alert')).alias("dec"),
    when(col('e_dec_detection').isNotNull(), col('e_dec_detection')).otherwise(col('e_dec_alert')).alias("e_dec"),
    when(col('e_mag_detection').isNotNull(), col('e_mag_detection')).otherwise(col('e_mag_alert')).alias("e_mag"),
    when(col('e_ra_detection').isNotNull(), col('e_ra_detection')).otherwise(col('e_ra_alert')).alias("e_ra"),
    when(col('fid_detection').isNotNull(), col('fid_detection')).otherwise(col('fid_alert')).alias("fid"),
    when(col('forced_detection').isNotNull(), col('forced_detection')).otherwise(col('forced_alert')).alias("forced"),
    when(col('has_stamp_detection').isNotNull(), col('has_stamp_detection')).otherwise(col('has_stamp_alert')).alias("has_stamp"),
    when(col('isdiffpos_detection').isNotNull(), col('isdiffpos_detection')).otherwise(col('isdiffpos_alert')).alias("isdiffpos"),
    when(col('mag_detection').isNotNull(), col('mag_detection')).otherwise(col('mag_alert')).alias("mag"),
    when(col('mjd_detection').isNotNull(), col('mjd_detection')).otherwise(col('mjd_alert')).alias("mjd"),
    when(col('parent_candid_detection').isNotNull(), col('parent_candid_detection')).otherwise(col('parent_candid_alert')).alias("parent_candid"),
    when(col('pid_detection').isNotNull(), col('pid_detection')).otherwise(col('pid_alert')).alias("pid"),
    when(col('ra_detection').isNotNull(), col('ra_detection')).otherwise(col('ra_alert')).alias("ra"),
    when(col('sid_detection').isNotNull(), col('sid_detection')).otherwise(col('sid_alert')).alias("sid"),
    when(col('tid_detection').isNotNull(), col('tid_detection')).otherwise(col('tid_alert')).alias("tid"),
    when(col('unparsed_fid_detection').isNotNull(), col('unparsed_fid_detection')).otherwise(col('unparsed_fid_alert')).alias("unparsed_fid"),
    when(col('unparsed_isdiffpos_detection').isNotNull(), col('unparsed_isdiffpos_detection')).otherwise(col('unparsed_isdiffpos_alert')).alias("unparsed_isdiffpos"),
    when(col('unparsed_jd_detection').isNotNull(), col('unparsed_jd_detection')).otherwise(col('unparsed_jd_alert')).alias("unparsed_jd"),
    when(col('aimage_detection').isNotNull(), col('aimage_detection')).otherwise(col('aimage_alert')).alias("aimage"),
    when(col('aimagerat_detection').isNotNull(), col('aimagerat_detection')).otherwise(col('aimagerat_alert')).alias("aimagerat"),
    when(col('bimage_detection').isNotNull(), col('bimage_detection')).otherwise(col('bimage_alert')).alias("bimage"),
    when(col('bimagerat_detection').isNotNull(), col('bimagerat_detection')).otherwise(col('bimagerat_alert')).alias("bimagerat"),
    when(col('chinr_detection').isNotNull(), col('chinr_detection')).otherwise(col('chinr_alert')).alias("chinr"),
    when(col('chipsf_detection').isNotNull(), col('chipsf_detection')).otherwise(col('chipsf_alert')).alias("chipsf"),
    when(col('classtar_detection').isNotNull(), col('classtar_detection')).otherwise(col('classtar_alert')).alias("classtar"),
    when(col('clrcoeff_detection').isNotNull(), col('clrcoeff_detection')).otherwise(col('clrcoeff_alert')).alias("clrcoeff"),
    when(col('clrcounc_detection').isNotNull(), col('clrcounc_detection')).otherwise(col('clrcounc_alert')).alias("clrcounc"),
    col("clrmed_alert").alias("clrmed"),
    col("clrrms_alert").alias("clrrms"),
    when(col('decnr_detection').isNotNull(), col('decnr_detection')).otherwise(col('decnr_alert')).alias("decnr"),
    when(col('diffmaglim_detection').isNotNull(), col('diffmaglim_detection')).otherwise(col('diffmaglim_alert')).alias("diffmaglim"),
    when(col('distnr_detection').isNotNull(), col('distnr_detection')).otherwise(col('distnr_alert')).alias("distnr"),
    col("distpsnr1_alert").alias("distpsnr1"),
    col("distpsnr2_alert").alias("distpsnr2"),
    col("distpsnr3_alert").alias("distpsnr3"),
    col("drb_alert").alias("drb"),
    col("drbversion_alert").alias("drbversion"),
    col("dsdiff_alert").alias("dsdiff"),
    col("dsnrms_alert").alias("dsnrms"),
    when(col('elong_detection').isNotNull(), col('elong_detection')).otherwise(col('elong_alert')).alias("elong"),
    col("exptime_alert").alias("exptime"),
    when(col('field_detection').isNotNull(), col('field_detection')).otherwise(col('field_alert')).alias("field"),
    when(col('fwhm_detection').isNotNull(), col('fwhm_detection')).otherwise(col('fwhm_alert')).alias("fwhm"),
    col("jdendhist_alert").alias("jdendhist"),
    col("jdendref_alert").alias("jdendref"),
    col("jdstarthist_alert").alias("jdstarthist"),
    col("jdstartref_alert").alias("jdstartref"),
    when(col('magap_detection').isNotNull(), col('magap_detection')).otherwise(col('magap_alert')).alias("magap"),
    when(col('magapbig_detection').isNotNull(), col('magapbig_detection')).otherwise(col('magapbig_alert')).alias("magapbig"),
    when(col('magdiff_detection').isNotNull(), col('magdiff_detection')).otherwise(col('magdiff_alert')).alias("magdiff"),
    when(col('magfromlim_detection').isNotNull(), col('magfromlim_detection')).otherwise(col('magfromlim_alert')).alias("magfromlim"),
    col("maggaia_alert").alias("maggaia"),
    col("maggaiabright_alert").alias("maggaiabright"),
    when(col('magnr_detection').isNotNull(), col('magnr_detection')).otherwise(col('magnr_alert')).alias("magnr"),
    when(col("magzpsci_detection").isNotNull(), col("magzpsci_detection")).otherwise(col("magzpsci_alert")).alias("magzpsci"),
    when(col("magzpscirms_detection").isNotNull(), col("magzpscirms_detection")).otherwise(col("magzpscirms_alert")).alias("magzpscirms"),
    when(col("magzpsciunc_detection").isNotNull(), col("magzpsciunc_detection")).otherwise(col("magzpsciunc_alert")).alias("magzpsciunc"),
    when(col("mindtoedge_detection").isNotNull(), col("mindtoedge_detection")).otherwise(col("mindtoedge_alert")).alias("mindtoedge"),
    when(col("nbad_detection").isNotNull(), col("nbad_detection")).otherwise(col("nbad_alert")).alias("nbad"),
    col("ncovhist_alert").alias("ncovhist"),
    col("ndethist_alert").alias("ndethist"),
    col("neargaia_alert").alias("neargaia"),
    col("neargaiabright_alert").alias("neargaiabright"),
    col("nframesref_alert").alias("nframesref"),
    when(col("nid_detection").isNotNull(), col("nid_detection")).otherwise(col("nid_alert")).alias("nid"),
    col("nmatches_alert").alias("nmatches"),
    col("nmtchps_alert").alias("nmtchps"),
    when(col("nneg_detection").isNotNull(), col("nneg_detection")).otherwise(col("nneg_alert")).alias("nneg"),
    col("objectidps1_alert").alias("objectidps1"),
    col("objectidps2_alert").alias("objectidps2"),
    col("objectidps3_alert").alias("objectidps3"),
    when(col("pdiffimfilename_detection").isNotNull(), col("pdiffimfilename_detection")).otherwise(col("pdiffimfilename_alert")).alias("pdiffimfilename"),
    when(col("programid_detection").isNotNull(), col("programid_detection")).otherwise(col("programid_alert")).alias("programid"),
    when(col("programpi_detection").isNotNull(), col("programpi_detection")).otherwise(col("programpi_alert")).alias("programpi"),
    when(col("ranr_detection").isNotNull(), col("ranr_detection")).otherwise(col("ranr_alert")).alias("ranr"),
    when(col("rb_detection").isNotNull(), col("rb_detection")).otherwise(col("rb_alert")).alias("rb"),
    when(col("rbversion_detection").isNotNull(), col("rbversion_detection")).otherwise(col("rbversion_alert")).alias("rbversion"),
    when(col("rcid_detection").isNotNull(), col("rcid_detection")).otherwise(col("rcid_alert")).alias("rcid"),
    col("rfid_alert").alias("rfid"),
    when(col("scorr_detection").isNotNull(), col("scorr_detection")).otherwise(col("scorr_alert")).alias("scorr"),
    when(col("seeratio_detection").isNotNull(), col("seeratio_detection")).otherwise(col("seeratio_alert")).alias("seeratio"),
    col("sgmag1_alert").alias("sgmag1"),
    col("sgmag2_alert").alias("sgmag2"),
    col("sgmag3_alert").alias("sgmag3"),
    col("sgscore1_alert").alias("sgscore1"),
    col("sgscore2_alert").alias("sgscore2"),
    col("sgscore3_alert").alias("sgscore3"),
    when(col("sharpnr_detection").isNotNull(), col("sharpnr_detection")).otherwise(col("sharpnr_alert")).alias("sharpnr"),
    when(col("sigmagap_detection").isNotNull(), col("sigmagap_detection")).otherwise(col("sigmagap_alert")).alias("sigmagap"),
    when(col("sigmagapbig_detection").isNotNull(), col("sigmagapbig_detection")).otherwise(col("sigmagapbig_alert")).alias("sigmagapbig"),
    when(col("sigmagnr_detection").isNotNull(), col("sigmagnr_detection")).otherwise(col("sigmagnr_alert")).alias("sigmagnr"),
    col("simag1_alert").alias("simag1"),
    col("simag2_alert").alias("simag2"),
    col("simag3_alert").alias("simag3"),
    when(col("sky_detection").isNotNull(), col("sky_detection")).otherwise(col("sky_alert")).alias("sky"),
    col("srmag1_alert").alias("srmag1"),
    col("srmag2_alert").alias("srmag2"),
    col("srmag3_alert").alias("srmag3"),
    when(col("ssdistnr_detection").isNotNull(), col("ssdistnr_detection")).otherwise(col("ssdistnr_alert")).alias("ssdistnr"),
    when(col("ssmagnr_detection").isNotNull(), col("ssmagnr_detection")).otherwise(col("ssmagnr_alert")).alias("ssmagnr"),
    when(col("ssnamenr_detection").isNotNull(), col("ssnamenr_detection")).otherwise(col("ssnamenr_alert")).alias("ssnamenr"),
    col("ssnrms_alert").alias("ssnrms"),
    when(col("sumrat_detection").isNotNull(), col("sumrat_detection")).otherwise(col("sumrat_alert")).alias("sumrat"),
    col("szmag1_alert").alias("szmag1"),
    col("szmag2_alert").alias("szmag2"),
    col("szmag3_alert").alias("szmag3"),
    when(col("tblid_detection").isNotNull(), col("tblid_detection")).otherwise(col("tblid_alert")).alias("tblid"),
    col("tooflag_alert").alias("tooflag"),
    when(col("xpos_detection").isNotNull(), col("xpos_detection")).otherwise(col("xpos_alert")).alias("xpos"),
    when(col("ypos_detection").isNotNull(), col("ypos_detection")).otherwise(col("ypos_alert")).alias("ypos"),
    col("zpclrcov_alert").alias("zpclrcov"),
    col("zpmed_alert").alias("zpmed"))

    # We generate the struct for the extra fields again

    not_extrafields = ['aid','candid','dec','e_dec', 'e_mag', 'e_ra', 'fid', 'forced', 'has_stamp', 'isdiffpos','mag', 'mjd', 'oid', 'parent_candid', 'pid', 'ra', 'sid', 'tid', 'unparsed_fid', 'unparsed_isdiffpos', 'unparsed_jd']
    df_extrafields = detections_dataframe.drop(*not_extrafields)
    sorted_columns_extra_fields = sorted(df_extrafields.columns)
    df_extrafields = df_extrafields.select(*sorted_columns_extra_fields)
    columns_extra_fields = df_extrafields.columns
    not_extrafields.append("extra_fields") # adding the extra fields to add to the detection dataframe
    detections_dataframe = detections_dataframe.withColumn("extra_fields", struct([col(c) for c in columns_extra_fields])).select(*[not_extrafields])
    sorted_columns_df = sorted(detections_dataframe.columns)
    detections_dataframe = detections_dataframe.select(*sorted_columns_df)
    detections_dataframe = detections_dataframe.repartition('oid')
    return detections_dataframe
