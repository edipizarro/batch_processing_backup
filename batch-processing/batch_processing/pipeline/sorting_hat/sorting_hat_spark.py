from ..spark_init.pyspark_configs import *


schema_avros = StructType([StructField('schemavsn', StringType(), True), StructField('publisher', StringType(), True), StructField('objectId', StringType(), True), StructField('candidate', StructType([StructField('jd', DoubleType(), True), StructField('fid', LongType(), True), StructField('pid', LongType(), True), StructField('diffmaglim', DoubleType(), True), StructField('pdiffimfilename', StringType(), True), StructField('programpi', StringType(), True), StructField('programid', LongType(), True), StructField('candid', LongType(), True), StructField('isdiffpos', StringType(), True), StructField('tblid', LongType(), True), StructField('nid', LongType(), True), StructField('rcid', LongType(), True), StructField('field', LongType(), True), StructField('xpos', DoubleType(), True), StructField('ypos', DoubleType(), True), StructField('ra', DoubleType(), True), StructField('dec', DoubleType(), True), StructField('magpsf', DoubleType(), True), StructField('sigmapsf', DoubleType(), True), StructField('chipsf', DoubleType(), True), StructField('magap', DoubleType(), True), StructField('sigmagap', DoubleType(), True), StructField('distnr', DoubleType(), True), StructField('magnr', DoubleType(), True), StructField('sigmagnr', DoubleType(), True), StructField('chinr', DoubleType(), True), StructField('sharpnr', DoubleType(), True), StructField('sky', DoubleType(), True), StructField('magdiff', DoubleType(), True), StructField('fwhm', DoubleType(), True), StructField('classtar', DoubleType(), True), StructField('mindtoedge', DoubleType(), True), StructField('magfromlim', DoubleType(), True), StructField('seeratio', DoubleType(), True), StructField('aimage', DoubleType(), True), StructField('bimage', DoubleType(), True), StructField('aimagerat', DoubleType(), True), StructField('bimagerat', DoubleType(), True), StructField('elong', DoubleType(), True), StructField('nneg', LongType(), True), StructField('nbad', LongType(), True), StructField('rb', DoubleType(), True), StructField('ssdistnr', DoubleType(), True), StructField('ssmagnr', DoubleType(), True), StructField('ssnamenr', StringType(), True), StructField('sumrat', DoubleType(), True), StructField('magapbig', DoubleType(), True), StructField('sigmagapbig', DoubleType(), True), StructField('ranr', DoubleType(), True), StructField('decnr', DoubleType(), True), StructField('sgmag1', DoubleType(), True), StructField('srmag1', DoubleType(), True), StructField('simag1', DoubleType(), True), StructField('szmag1', DoubleType(), True), StructField('sgscore1', DoubleType(), True), StructField('distpsnr1', DoubleType(), True), StructField('ndethist', LongType(), True), StructField('ncovhist', LongType(), True), StructField('jdstarthist', DoubleType(), True), StructField('jdendhist', DoubleType(), True), StructField('scorr', DoubleType(), True), StructField('tooflag', LongType(), True), StructField('objectidps1', LongType(), True), StructField('objectidps2', LongType(), True), StructField('sgmag2', DoubleType(), True), StructField('srmag2', DoubleType(), True), StructField('simag2', DoubleType(), True), StructField('szmag2', DoubleType(), True), StructField('sgscore2', DoubleType(), True), StructField('distpsnr2', DoubleType(), True), StructField('objectidps3', LongType(), True), StructField('sgmag3', DoubleType(), True), StructField('srmag3', DoubleType(), True), StructField('simag3', DoubleType(), True), StructField('szmag3', DoubleType(), True), StructField('sgscore3', DoubleType(), True), StructField('distpsnr3', DoubleType(), True), StructField('nmtchps', LongType(), True), StructField('rfid', LongType(), True), StructField('jdstartref', DoubleType(), True), StructField('jdendref', DoubleType(), True), StructField('nframesref', LongType(), True), StructField('rbversion', StringType(), True), StructField('dsnrms', DoubleType(), True), StructField('ssnrms', DoubleType(), True), StructField('dsdiff', DoubleType(), True), StructField('magzpsci', DoubleType(), True), StructField('magzpsciunc', DoubleType(), True), StructField('magzpscirms', DoubleType(), True), StructField('nmatches', LongType(), True), StructField('clrcoeff', DoubleType(), True), StructField('clrcounc', DoubleType(), True), StructField('zpclrcov', DoubleType(), True), StructField('zpmed', DoubleType(), True), StructField('clrmed', DoubleType(), True), StructField('clrrms', DoubleType(), True), StructField('neargaia', DoubleType(), True), StructField('neargaiabright', DoubleType(), True), StructField('maggaia', DoubleType(), True), StructField('maggaiabright', DoubleType(), True), StructField('exptime', DoubleType(), True), StructField('drb', DoubleType(), True), StructField('drbversion', StringType(), True)]), True), StructField('prv_candidates', ArrayType(StructType([StructField('jd', DoubleType(), True), StructField('fid', LongType(), True), StructField('pid', LongType(), True), StructField('diffmaglim', DoubleType(), True), StructField('pdiffimfilename', StringType(), True), StructField('programpi', StringType(), True), StructField('programid', LongType(), True), StructField('candid', LongType(), True), StructField('isdiffpos', StringType(), True), StructField('tblid', LongType(), True), StructField('nid', LongType(), True), StructField('rcid', LongType(), True), StructField('field', LongType(), True), StructField('xpos', DoubleType(), True), StructField('ypos', DoubleType(), True), StructField('ra', DoubleType(), True), StructField('dec', DoubleType(), True), StructField('magpsf', DoubleType(), True), StructField('sigmapsf', DoubleType(), True), StructField('chipsf', DoubleType(), True), StructField('magap', DoubleType(), True), StructField('sigmagap', DoubleType(), True), StructField('distnr', DoubleType(), True), StructField('magnr', DoubleType(), True), StructField('sigmagnr', DoubleType(), True), StructField('chinr', DoubleType(), True), StructField('sharpnr', DoubleType(), True), StructField('sky', DoubleType(), True), StructField('magdiff', DoubleType(), True), StructField('fwhm', DoubleType(), True), StructField('classtar', DoubleType(), True), StructField('mindtoedge', DoubleType(), True), StructField('magfromlim', DoubleType(), True), StructField('seeratio', DoubleType(), True), StructField('aimage', DoubleType(), True), StructField('bimage', DoubleType(), True), StructField('aimagerat', DoubleType(), True), StructField('bimagerat', DoubleType(), True), StructField('elong', DoubleType(), True), StructField('nneg', LongType(), True), StructField('nbad', LongType(), True), StructField('rb', DoubleType(), True), StructField('ssdistnr', DoubleType(), nullable=True), StructField('ssmagnr', DoubleType(), True), StructField('ssnamenr', StringType(), True), StructField('sumrat', DoubleType(), True), StructField('magapbig', DoubleType(), True), StructField('sigmagapbig', DoubleType(), True), StructField('ranr', DoubleType(), True), StructField('decnr', DoubleType(), True), StructField('scorr', DoubleType(), True), StructField('magzpsci', DoubleType(), True), StructField('magzpsciunc', DoubleType(), True), StructField('magzpscirms', DoubleType(), True), StructField('clrcoeff', DoubleType(), True), StructField('clrcounc', DoubleType(), True), StructField('rbversion', StringType(), True)]), True), True), StructField('fp_hists', ArrayType(StructType([StructField('field', LongType(), True), StructField('rcid', LongType(), True), StructField('fid', LongType(), True), StructField('pid', LongType(), True), StructField('rfid', LongType(), True), StructField('sciinpseeing', DoubleType(), True), StructField('scibckgnd', DoubleType(), True), StructField('scisigpix', DoubleType(), True), StructField('magzpsci', DoubleType(), True), StructField('magzpsciunc', DoubleType(), True), StructField('magzpscirms', DoubleType(), True), StructField('clrcoeff', DoubleType(), True), StructField('clrcounc', DoubleType(), True), StructField('exptime', DoubleType(), True), StructField('adpctdif1', DoubleType(), True), StructField('adpctdif2', DoubleType(), True), StructField('diffmaglim', DoubleType(), True), StructField('programid', LongType(), True), StructField('jd', DoubleType(), True), StructField('forcediffimflux', DoubleType(), True), StructField('forcediffimfluxunc', DoubleType(), True), StructField('procstatus', StringType(), True), StructField('distnr', DoubleType(), True), StructField('ranr', DoubleType(), True), StructField('decnr', DoubleType(), True), StructField('magnr', DoubleType(), True), StructField('sigmagnr', DoubleType(), True), StructField('chinr', DoubleType(), True), StructField('sharpnr', DoubleType(), True)]), True), True)])


#First we must load all the parquets containing the avros
def load_dataframes(parquet_avro_dir, spark):
    parquetDataFrame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(schema_avros).load(parquet_avro_dir).select(
    col("objectId").alias("objectId"),
    col("prv_candidates").alias("prv_candidates"),
    col("fp_hists").alias("fp_hists"),
    col("candidate.jd").alias("jd"),
    col("candidate.fid").alias("fid"),
    col("candidate.pid").alias("pid"),
    col("candidate.diffmaglim").alias("diffmaglim"),
    col("candidate.pdiffimfilename").alias("pdiffimfilename"),
    col("candidate.programpi").alias("programpi"),
    col("candidate.programid").alias("programid"),
    col("candidate.candid").alias("candid"),
    col("candidate.isdiffpos").alias("isdiffpos"),
    col("candidate.tblid").alias("tblid"),
    col("candidate.nid").alias("nid"),
    col("candidate.rcid").alias("rcid"),
    col("candidate.field").alias("field"),
    col("candidate.xpos").alias("xpos"),
    col("candidate.ypos").alias("ypos"),
    col("candidate.ra").alias("ra"),
    col("candidate.dec").alias("dec"),
    col("candidate.magpsf").alias("magpsf"),
    col("candidate.sigmapsf").alias("sigmapsf"),
    col("candidate.chipsf").alias("chipsf"),
    col("candidate.magap").alias("magap"),
    col("candidate.sigmagap").alias("sigmagap"),
    col("candidate.distnr").alias("distnr"),
    col("candidate.magnr").alias("magnr"),
    col("candidate.sigmagnr").alias("sigmagnr"),
    col("candidate.chinr").alias("chinr"),
    col("candidate.sharpnr").alias("sharpnr"),
    col("candidate.sky").alias("sky"),
    col("candidate.magdiff").alias("magdiff"),
    col("candidate.fwhm").alias("fwhm"),
    col("candidate.classtar").alias("classtar"),
    col("candidate.mindtoedge").alias("mindtoedge"),
    col("candidate.magfromlim").alias("magfromlim"),
    col("candidate.seeratio").alias("seeratio"),
    col("candidate.aimage").alias("aimage"),
    col("candidate.bimage").alias("bimage"),
    col("candidate.aimagerat").alias("aimagerat"),
    col("candidate.bimagerat").alias("bimagerat"),
    col("candidate.elong").alias("elong"),
    col("candidate.nneg").alias("nneg"),
    col("candidate.nbad").alias("nbad"),
    col("candidate.rb").alias("rb"),
    col("candidate.ssdistnr").alias("ssdistnr"),
    col("candidate.ssmagnr").alias("ssmagnr"),
    col("candidate.ssnamenr").alias("ssnamenr"),
    col("candidate.sumrat").alias("sumrat"),
    col("candidate.magapbig").alias("magapbig"),
    col("candidate.sigmagapbig").alias("sigmagapbig"),
    col("candidate.ranr").alias("ranr"),
    col("candidate.decnr").alias("decnr"),
    col("candidate.sgmag1").alias("sgmag1"),
    col("candidate.srmag1").alias("srmag1"),
    col("candidate.simag1").alias("simag1"),
    col("candidate.szmag1").alias("szmag1"),
    col("candidate.sgscore1").alias("sgscore1"),
    col("candidate.distpsnr1").alias("distpsnr1"),
    col("candidate.objectidps1").alias("objectidps1"),
    col("candidate.objectidps2").alias("objectidps2"),
    col("candidate.sgmag2").alias("sgmag2"),
    col("candidate.srmag2").alias("srmag2"),
    col("candidate.simag2").alias("simag2"),
    col("candidate.szmag2").alias("szmag2"),
    col("candidate.sgscore2").alias("sgscore2"),
    col("candidate.distpsnr2").alias("distpsnr2"),
    col("candidate.objectidps3").alias("objectidps3"),
    col("candidate.sgmag3").alias("sgmag3"),
    col("candidate.srmag3").alias("srmag3"),
    col("candidate.simag3").alias("simag3"),
    col("candidate.szmag3").alias("szmag3"),
    col("candidate.sgscore3").alias("sgscore3"),
    col("candidate.distpsnr3").alias("distpsnr3"),
    col("candidate.nmtchps").alias("nmtchps"),
    col("candidate.rfid").alias("rfid"),
    col("candidate.jdstarthist").alias("jdstarthist"),
    col("candidate.jdendhist").alias("jdendhist"),
    col("candidate.scorr").alias("scorr"),
    col("candidate.tooflag").alias("tooflag"),
    col("candidate.drbversion").alias("drbversion"),
    col("candidate.rbversion").alias("rbversion"),
    col("candidate.ndethist").alias("ndethist"),
    col("candidate.ncovhist").alias("ncovhist"),
    col("candidate.jdstartref").alias("jdstartref"),
    col("candidate.jdendref").alias("jdendref"),
    col("candidate.nframesref").alias("nframesref"),
    col("candidate.dsnrms").alias("dsnrms"),
    col("candidate.ssnrms").alias("ssnrms"),
    col("candidate.dsdiff").alias("dsdiff"),
    col("candidate.magzpsci").alias("magzpsci"),
    col("candidate.magzpsciunc").alias("magzpsciunc"),
    col("candidate.magzpscirms").alias("magzpscirms"),
    col("candidate.nmatches").alias("nmatches"),
    col("candidate.clrcoeff").alias("clrcoeff"),
    col("candidate.clrcounc").alias("clrcounc"),
    col("candidate.zpclrcov").alias("zpclrcov"),
    col("candidate.zpmed").alias("zpmed"),
    col("candidate.clrmed").alias("clrmed"),
    col("candidate.clrrms").alias("clrrms"),
    col("candidate.neargaia").alias("neargaia"),
    col("candidate.neargaiabright").alias("neargaiabright"),
    col("candidate.maggaia").alias("maggaia"),
    col("candidate.maggaiabright").alias("maggaiabright"),
    col("candidate.exptime").alias("exptime"),
    col("candidate.drb").alias("drb"))
    return parquetDataFrame


# We define the structure of the parsing dataframe function
def df_sorting_hat(df):
    df = _parse_ztf_df(df)
    df2 = alerce_id_generator(df)
    df3 = aid_replacer(df2)
    return df3

def _parse_ztf_df(df):
    df = _apply_transformations(df)
    #df = _parse_extrafields(df)
    return df


def _apply_transformations(df):
    df = df.select(
        F.col("objectID").alias('oid'),
        F.col("prv_candidates"),
        F.col("fp_hists"),
        F.col("jd").alias("unparsed_jd"),
        F.col("fid").alias("unparsed_fid"),
        F.col("pid"),
        F.col("diffmaglim"),
        F.col("pdiffimfilename"),
        F.col("programpi"),
        F.col("programid"),
        F.col("candid").cast(StringType()),
        F.when(F.col("isdiffpos") == "t", 1).when(F.col("isdiffpos") == "1", 1).otherwise(-1).alias("isdiffpos"),
        F.col("tblid"),
        F.col("nid"),
        F.col("rcid"),
        F.col("field"),
        F.col("xpos"),
        F.col("ypos"),
        F.col("ra"),
        F.col("dec"),
        F.col("magpsf").alias('mag'),
        F.col("sigmapsf").alias('e_mag'),
        F.col("chipsf"),
        F.col("magap"),
        F.col("sigmagap"),
        F.col("distnr"),
        F.col("magnr"),
        F.col("sigmagnr"),
        F.col("chinr"),
        F.col("sharpnr"),
        F.col("sky"),
        F.col("magdiff"),
        F.col("fwhm"),
        F.col("classtar"),
        F.col("mindtoedge"),
        F.col("magfromlim"),
        F.col("seeratio"),
        F.col("aimage"),
        F.col("bimage"),
        F.col("aimagerat"),
        F.col("bimagerat"),
        F.col("elong"),
        F.col("nneg"),
        F.col("nbad"),
        F.col("rb"),
        F.col("ssdistnr"),
        F.col("ssmagnr"),
        F.col("ssnamenr"),
        F.col("sumrat"),
        F.col("magapbig"),
        F.col("sigmagapbig"),
        F.col("ranr"),
        F.col("decnr"),
        F.col("sgmag1"),
        F.col("srmag1"),
        F.col("simag1"),
        F.col("szmag1"),
        F.col("sgscore1"),
        F.col("distpsnr1"),
        F.col("objectidps1"),
        F.col("objectidps2"),
        F.col("sgmag2"),
        F.col("srmag2"),
        F.col("simag2"),
        F.col("szmag2"),
        F.col("sgscore2"),
        F.col("distpsnr2"),
        F.col("objectidps3"),
        F.col("sgmag3"),
        F.col("srmag3"),
        F.col("simag3"),
        F.col("szmag3"),
        F.col("sgscore3"),
        F.col("distpsnr3"),
        F.col("nmtchps"),
        F.col("rfid"),
        F.col("jdstarthist"),
        F.col("jdendhist"),
        F.col("scorr"),
        F.col("tooflag"),
        F.col("drbversion"),
        F.col("rbversion"),
        F.col("ndethist"),
        F.col("ncovhist"),
        F.col("jdstartref"),
        F.col("jdendref"),
        F.col("nframesref"),
        F.col("dsnrms"),
        F.col("ssnrms"),
        F.col("dsdiff"),
        F.col("magzpsci"),
        F.col("magzpsciunc"),
        F.col("magzpscirms"),
        F.col("nmatches"),
        F.col("clrcoeff"),
        F.col("clrcounc"),
        F.col("zpclrcov"),
        F.col("zpmed"),
        F.col("clrmed"),
        F.col("clrrms"),
        F.col("neargaia"),
        F.col("neargaiabright"),
        F.col("maggaia"),
        F.col("maggaiabright"),
        F.col("exptime"),
        F.col("drb"),
        F.lit("ZTF").alias("tid"),
        F.lit("ZTF").alias("sid"),
        F.when(F.col("unparsed_fid") == 1, "g").when(F.col("unparsed_fid") == 2, "r").otherwise("i").alias("fid"),
        (F.col("jd") - 2400000.5).alias("mjd"),
        F.col("isdiffpos").alias("unparsed_isdiffpos"),
        F.when(F.col("fid") == 1, 0.065).when(F.col("fid") == 2, 0.085).otherwise(0.01).alias("e_dec_decimal"),
        F.when(F.col("fid") == 1, 0.06499999761581421).when(F.col("fid") == 2, 0.08500000089406967).otherwise(0.01).alias("e_dec"),
        F.when(F.cos(F.radians(F.col("dec"))) != 0, F.col("e_dec_decimal") / F.abs(F.cos(F.radians(F.col("dec"))))).otherwise(float('nan')).alias("e_ra")
    )
    df = df.drop('e_dec_decimal')

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
    df= df.withColumn("e_ra", format_and_convert_udf(df["e_ra"]))

# 




    return df

#! missing true value of 0.01

############################### SORTING HAT AID GENERATION ##############################################
def alerce_id_generator(df):
    # Fix negative Ra
    df = df.select(
        "*",
        when(col("ra") < 0, col("ra") + 360)
            .when(col("ra") > 360, col("ra") - 360)
            .otherwise(col("ra")).alias("ra_fixed")
    )

    # Calculate Ra components
    df = df.select(
        "*",
        ((col("ra_fixed") / 15).cast("bigint")).alias("ra_hh"),
        (((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60).cast("bigint").alias("ra_mm"),
        ((((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60 - ((((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60).cast("bigint"))) * 60).cast("bigint").alias("ra_ss"),
        (((((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60 - ((((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60).cast("bigint"))) * 60 - (((((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60 - ((((col("ra_fixed") / 15) - ((col("ra_fixed") / 15).cast("bigint"))) * 60).cast("bigint"))) * 60).cast("bigint"))).alias("ra_ff")
    ))

    # Fix negative Dec
    df = df.select(
        "*",
        when(col("dec") >= 0, lit(1)).otherwise(lit(0)).alias("h"),
        abs(col("dec")).alias("dec_fixed")
    )

    # Calculate Dec components
    # Calculate Dec components
    df = df.select(
        "*",
    (col("dec_fixed") / 15).cast("bigint").alias("dec_deg"),
    (((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60).cast("bigint").alias("dec_mm"),
    (((((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60) - (((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60).cast("bigint")) * 60).cast("bigint").alias("dec_ss"),
    ((((((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60) - (((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60).cast("bigint")) * 60) - ((((((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60) - (((col("dec_fixed") / 15) - (col("dec_fixed") / 15).cast("bigint")) * 60).cast("bigint")) * 60).cast("bigint"))).alias("dec_f")
    )

    # Calculate the aid
    df = df.select(
        "*",
        (lit(1000000000000000000) +
         (col("ra_hh") * 10000000000000000) +
         (col("ra_mm") * 100000000000000) +
         (col("ra_ss") * 1000000000000) +
         (col("ra_ff") * 10000000000) +
         (col("h") * 1000000000) +
         (col("dec_deg") * 10000000) +
         (col("dec_mm") * 100000) +
         (col("dec_ss") * 1000) +
         (col("dec_f") * 100)).cast(StringType()).alias("aid")
    )

    # Drop intermediate columns
    df = df.drop("ra_hh", "ra_mm", "ra_ss", "ra_ff", "ra_fixed","h", "dec_deg", "dec_mm", "dec_ss", "dec_f", "dec_fixed")
    
    return df


def aid_replacer(df):
    df = df.repartition('oid')
    window_spec = Window.partitionBy("oid").orderBy("mjd")
    df = df.withColumn("aid_first_mjd", first("aid").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    df = df.drop("aid").withColumnRenamed("aid_first_mjd", "aid")
    return df

############################### EXECUTE ##############################################

def run_sorting_hat_step(avro_parquets_dir, spark):
    df = load_dataframes(avro_parquets_dir, spark)
    df = df_sorting_hat(df)
    sorted_columns_df = sorted(df.columns)
    df = df.select(*sorted_columns_df)
    return df

