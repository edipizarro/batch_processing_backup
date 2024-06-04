import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cos, radians, when, abs, struct

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, IntegerType, ShortType
spark = SparkSession.builder.config("spark.driver.host", "localhost").config("spark.driver.memory", "6g").appName("SparkExample").getOrCreate()
conf = pyspark.SparkConf()
from pyspark.sql.functions import lit

spark_context = SparkSession.builder.config(conf=conf).getOrCreate()

schema_avros = StructType([StructField('schemavsn', StringType(), True), StructField('publisher', StringType(), True), StructField('objectId', StringType(), True), StructField('candidate', StructType([StructField('jd', DoubleType(), True), StructField('fid', LongType(), True), StructField('pid', LongType(), True), StructField('diffmaglim', DoubleType(), True), StructField('pdiffimfilename', StringType(), True), StructField('programpi', StringType(), True), StructField('programid', LongType(), True), StructField('candid', LongType(), True), StructField('isdiffpos', StringType(), True), StructField('tblid', LongType(), True), StructField('nid', LongType(), True), StructField('rcid', LongType(), True), StructField('field', LongType(), True), StructField('xpos', DoubleType(), True), StructField('ypos', DoubleType(), True), StructField('ra', DoubleType(), True), StructField('dec', DoubleType(), True), StructField('magpsf', DoubleType(), True), StructField('sigmapsf', DoubleType(), True), StructField('chipsf', DoubleType(), True), StructField('magap', DoubleType(), True), StructField('sigmagap', DoubleType(), True), StructField('distnr', DoubleType(), True), StructField('magnr', DoubleType(), True), StructField('sigmagnr', DoubleType(), True), StructField('chinr', DoubleType(), True), StructField('sharpnr', DoubleType(), True), StructField('sky', DoubleType(), True), StructField('magdiff', DoubleType(), True), StructField('fwhm', DoubleType(), True), StructField('classtar', DoubleType(), True), StructField('mindtoedge', DoubleType(), True), StructField('magfromlim', DoubleType(), True), StructField('seeratio', DoubleType(), True), StructField('aimage', DoubleType(), True), StructField('bimage', DoubleType(), True), StructField('aimagerat', DoubleType(), True), StructField('bimagerat', DoubleType(), True), StructField('elong', DoubleType(), True), StructField('nneg', LongType(), True), StructField('nbad', LongType(), True), StructField('rb', DoubleType(), True), StructField('ssdistnr', DoubleType(), True), StructField('ssmagnr', DoubleType(), True), StructField('ssnamenr', StringType(), True), StructField('sumrat', DoubleType(), True), StructField('magapbig', DoubleType(), True), StructField('sigmagapbig', DoubleType(), True), StructField('ranr', DoubleType(), True), StructField('decnr', DoubleType(), True), StructField('sgmag1', DoubleType(), True), StructField('srmag1', DoubleType(), True), StructField('simag1', DoubleType(), True), StructField('szmag1', DoubleType(), True), StructField('sgscore1', DoubleType(), True), StructField('distpsnr1', DoubleType(), True), StructField('ndethist', LongType(), True), StructField('ncovhist', LongType(), True), StructField('jdstarthist', DoubleType(), True), StructField('jdendhist', DoubleType(), True), StructField('scorr', DoubleType(), True), StructField('tooflag', LongType(), True), StructField('objectidps1', LongType(), True), StructField('objectidps2', LongType(), True), StructField('sgmag2', DoubleType(), True), StructField('srmag2', DoubleType(), True), StructField('simag2', DoubleType(), True), StructField('szmag2', DoubleType(), True), StructField('sgscore2', DoubleType(), True), StructField('distpsnr2', DoubleType(), True), StructField('objectidps3', LongType(), True), StructField('sgmag3', DoubleType(), True), StructField('srmag3', DoubleType(), True), StructField('simag3', DoubleType(), True), StructField('szmag3', DoubleType(), True), StructField('sgscore3', DoubleType(), True), StructField('distpsnr3', DoubleType(), True), StructField('nmtchps', LongType(), True), StructField('rfid', LongType(), True), StructField('jdstartref', DoubleType(), True), StructField('jdendref', DoubleType(), True), StructField('nframesref', LongType(), True), StructField('rbversion', StringType(), True), StructField('dsnrms', DoubleType(), True), StructField('ssnrms', DoubleType(), True), StructField('dsdiff', DoubleType(), True), StructField('magzpsci', DoubleType(), True), StructField('magzpsciunc', DoubleType(), True), StructField('magzpscirms', DoubleType(), True), StructField('nmatches', LongType(), True), StructField('clrcoeff', DoubleType(), True), StructField('clrcounc', DoubleType(), True), StructField('zpclrcov', DoubleType(), True), StructField('zpmed', DoubleType(), True), StructField('clrmed', DoubleType(), True), StructField('clrrms', DoubleType(), True), StructField('neargaia', DoubleType(), True), StructField('neargaiabright', DoubleType(), True), StructField('maggaia', DoubleType(), True), StructField('maggaiabright', DoubleType(), True), StructField('exptime', DoubleType(), True), StructField('drb', DoubleType(), True), StructField('drbversion', StringType(), True)]), True), StructField('prv_candidates', ArrayType(StructType([StructField('jd', DoubleType(), True), StructField('fid', LongType(), True), StructField('pid', LongType(), True), StructField('diffmaglim', DoubleType(), True), StructField('pdiffimfilename', StringType(), True), StructField('programpi', StringType(), True), StructField('programid', LongType(), True), StructField('candid', LongType(), True), StructField('isdiffpos', StringType(), True), StructField('tblid', LongType(), True), StructField('nid', LongType(), True), StructField('rcid', LongType(), True), StructField('field', LongType(), True), StructField('xpos', DoubleType(), True), StructField('ypos', DoubleType(), True), StructField('ra', DoubleType(), True), StructField('dec', DoubleType(), True), StructField('magpsf', DoubleType(), True), StructField('sigmapsf', DoubleType(), True), StructField('chipsf', DoubleType(), True), StructField('magap', DoubleType(), True), StructField('sigmagap', DoubleType(), True), StructField('distnr', DoubleType(), True), StructField('magnr', DoubleType(), True), StructField('sigmagnr', DoubleType(), True), StructField('chinr', DoubleType(), True), StructField('sharpnr', DoubleType(), True), StructField('sky', DoubleType(), True), StructField('magdiff', DoubleType(), True), StructField('fwhm', DoubleType(), True), StructField('classtar', DoubleType(), True), StructField('mindtoedge', DoubleType(), True), StructField('magfromlim', DoubleType(), True), StructField('seeratio', DoubleType(), True), StructField('aimage', DoubleType(), True), StructField('bimage', DoubleType(), True), StructField('aimagerat', DoubleType(), True), StructField('bimagerat', DoubleType(), True), StructField('elong', DoubleType(), True), StructField('nneg', LongType(), True), StructField('nbad', LongType(), True), StructField('rb', DoubleType(), True), StructField('ssdistnr', DoubleType(), nullable=True), StructField('ssmagnr', DoubleType(), True), StructField('ssnamenr', StringType(), True), StructField('sumrat', DoubleType(), True), StructField('magapbig', DoubleType(), True), StructField('sigmagapbig', DoubleType(), True), StructField('ranr', DoubleType(), True), StructField('decnr', DoubleType(), True), StructField('scorr', DoubleType(), True), StructField('magzpsci', DoubleType(), True), StructField('magzpsciunc', DoubleType(), True), StructField('magzpscirms', DoubleType(), True), StructField('clrcoeff', DoubleType(), True), StructField('clrcounc', DoubleType(), True), StructField('rbversion', StringType(), True)]), True), True), StructField('fp_hists', ArrayType(StructType([StructField('field', LongType(), True), StructField('rcid', LongType(), True), StructField('fid', LongType(), True), StructField('pid', LongType(), True), StructField('rfid', LongType(), True), StructField('sciinpseeing', DoubleType(), True), StructField('scibckgnd', DoubleType(), True), StructField('scisigpix', DoubleType(), True), StructField('magzpsci', DoubleType(), True), StructField('magzpsciunc', DoubleType(), True), StructField('magzpscirms', DoubleType(), True), StructField('clrcoeff', DoubleType(), True), StructField('clrcounc', DoubleType(), True), StructField('exptime', DoubleType(), True), StructField('adpctdif1', DoubleType(), True), StructField('adpctdif2', DoubleType(), True), StructField('diffmaglim', DoubleType(), True), StructField('programid', LongType(), True), StructField('jd', DoubleType(), True), StructField('forcediffimflux', DoubleType(), True), StructField('forcediffimfluxunc', DoubleType(), True), StructField('procstatus', StringType(), True), StructField('distnr', DoubleType(), True), StructField('ranr', DoubleType(), True), StructField('decnr', DoubleType(), True), StructField('magnr', DoubleType(), True), StructField('sigmagnr', DoubleType(), True), StructField('chinr', DoubleType(), True), StructField('sharpnr', DoubleType(), True)]), True), True)])


#First we must load all the parquets containing the avros
def load_dataframes(parquet_avro_dir):
    parquetDataFrame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(schema_avros).load(parquet_avro_dir)
    return parquetDataFrame


# We define the structure of the parsing dataframe function
def df_sorting_hat(df):
    df = _parse_ztf_df(df)

    df = alerce_id_generator(df)
    df = aid_replacer(df)
    return df

def _parse_ztf_df(df):
    df = _drop_unnecessary_fields(df)
    df = _unnest_candidate(df)
    df = _apply_transformations(df)
    df = _parse_extrafields(df)
    return df

def _drop_unnecessary_fields(df):
    fields_to_drop = [
        "schemavsn",
        "publisher",
        "cutoutScience",
        "cutoutTemplate",
        "cutoutDifference"
    ]
    df = df.drop(*fields_to_drop)
    return df

def _unnest_candidate(df):
    df = df.selectExpr(
        "objectId",
        "prv_candidates",
        "fp_hists",
        "candidate.jd",
        "candidate.fid",
        "candidate.pid",
        "candidate.diffmaglim",
        "candidate.pdiffimfilename",
        "candidate.programpi",
        "candidate.programid",
        "candidate.candid",
        "candidate.isdiffpos",
        "candidate.tblid",
        "candidate.nid",
        "candidate.rcid",
        "candidate.field",
        "candidate.xpos",
        "candidate.ypos",
        "candidate.ra",
        "candidate.dec",
        "candidate.magpsf",
        "candidate.sigmapsf",
        "candidate.chipsf",
        "candidate.magap",
        "candidate.sigmagap",
        "candidate.distnr",
        "candidate.magnr",
        "candidate.sigmagnr",
        "candidate.chinr",
        "candidate.sharpnr",
        "candidate.sky",
        "candidate.magdiff",
        "candidate.fwhm",
        "candidate.classtar",
        "candidate.mindtoedge",
        "candidate.magfromlim",
        "candidate.seeratio",
        "candidate.aimage",
        "candidate.bimage",
        "candidate.aimagerat",
        "candidate.bimagerat",
        "candidate.elong",
        "candidate.nneg",
        "candidate.nbad",
        "candidate.rb",
        "candidate.ssdistnr",
        "candidate.ssmagnr",
        "candidate.ssnamenr",
        "candidate.sumrat",
        "candidate.magapbig",
        "candidate.sigmagapbig",
        "candidate.ranr",
        "candidate.decnr",
        "candidate.sgmag1",
        "candidate.srmag1",
        "candidate.simag1",
        "candidate.szmag1",
        "candidate.sgscore1",
        "candidate.distpsnr1",
        "candidate.objectidps1",
        "candidate.objectidps2",
        "candidate.sgmag2",
        "candidate.srmag2",
        "candidate.simag2",
        "candidate.szmag2",
        "candidate.sgscore2",
        "candidate.distpsnr2",
        "candidate.objectidps3",
        "candidate.sgmag3",
        "candidate.srmag3",
        "candidate.simag3",
        "candidate.szmag3",
        "candidate.sgscore3",
        "candidate.distpsnr3",
        "candidate.nmtchps",
        "candidate.rfid",
        "candidate.jdstarthist",
        "candidate.jdendhist",
        "candidate.scorr",
        "candidate.tooflag",
        "candidate.drbversion",
        "candidate.rbversion",
        "candidate.ndethist",
        "candidate.ncovhist",
        "candidate.jdstartref",
        "candidate.jdendref",
        "candidate.nframesref",
        "candidate.dsnrms",
        "candidate.ssnrms",
        "candidate.dsdiff",
        "candidate.magzpsci",
        "candidate.magzpsciunc",
        "candidate.magzpscirms",
        "candidate.nmatches",
        "candidate.clrcoeff",
        "candidate.clrcounc",
        "candidate.zpclrcov",
        "candidate.zpmed",
        "candidate.clrmed",
        "candidate.clrrms",
        "candidate.neargaia",
        "candidate.neargaiabright",
        "candidate.maggaia",
        "candidate.maggaiabright",
        "candidate.exptime",
        "candidate.drb"
    )
    return df

def _apply_transformations(df):
    df = df.select(
        F.col("objectID").alias('oid'),
        F.col("prv_candidates"),
        F.col("fp_hists"),
        F.col("jd").alias("unparsed_jd"),
        F.col("fid"),
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
        F.when(F.col("fid") == 1, "g").when(F.col("fid") == 2, "r").otherwise("i").alias("unparsed_fid"),
        (F.col("jd") - 2400000.5).alias("mjd"),
        F.col("unparsed_fid"),
        F.col("isdiffpos").alias("unparsed_isdiffpos"),
        F.when(F.col("fid") == "g", 0.065).when(F.col("fid") == "r", 0.085).otherwise(0.01).alias("e_dec"),
        F.when(F.cos(F.radians(F.col("dec"))) != 0, F.col("e_dec") / F.abs(F.cos(F.radians(F.col("dec"))))).otherwise(float('nan')).alias("e_ra")
    )
    return df

def _parse_extrafields(df):
    not_extrafields = [
        "oid",
        "tid",
        "sid",
        "pid",
        "candid",
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
    ### We use this one to be able to join up the columns in the place they correspond
    not_extrafields_withid = [
        "tid",
        "sid",
        "pid",
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

    df_extrafields = df.drop(*not_extrafields_withid)
    sorted_columns_extra_fields = sorted(df_extrafields.columns)
    df_extrafields = df_extrafields.select(*sorted_columns_extra_fields)
    columns_extra_fields = df_extrafields.columns

    df_extrafields = df_extrafields.select(
        F.col("oid"),
        F.col("candid"),
        F.struct(*[F.col(c) for c in columns_extra_fields if c not in ["oid", "candid"]]).alias("extra_fields")
    )
    df = df.select(*not_extrafields)
    df = df.join(df_extrafields, on=["oid", "candid"])
    sorted_columns_df = sorted(df.columns)
    df = df.select(*sorted_columns_df)

    return df

############################### SORTING HAT AID GENERATION ##############################################
def alerce_id_generator(df):
    # Fix negative Ra
    df = df.select(
        "*",
        when(col("ra") < 0, col("ra") + 360)
            .when(col("ra") > 360, col("ra") - 360)
            .otherwise(col("ra")).alias("ra_fixed")
    ).drop("ra").withColumnRenamed("ra_fixed", "ra")

    # Calculate Ra components
    df = df.select(
        "*",
        ((col("ra") / 15).cast("bigint")).alias("ra_hh"),
        (((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60).cast("bigint").alias("ra_mm"),
        ((((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60 - ((((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60).cast("bigint"))) * 60).cast("bigint").alias("ra_ss"),
        (((((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60 - ((((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60).cast("bigint"))) * 60 - (((((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60 - ((((col("ra") / 15) - ((col("ra") / 15).cast("bigint"))) * 60).cast("bigint"))) * 60).cast("bigint"))).alias("ra_ff")
    ))

    # Fix negative Dec
    df = df.select(
        "*",
        when(col("dec") >= 0, lit(1)).otherwise(lit(0)).alias("h"),
        abs(col("dec")).alias("dec_fixed")
    ).drop("dec").withColumnRenamed("dec_fixed", "dec")

    # Calculate Dec components
    # Calculate Dec components
    df = df.select(
        "*",
    (col("dec") / 15).cast("bigint").alias("dec_deg"),
    (((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60).cast("bigint").alias("dec_mm"),
    (((((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60) - (((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60).cast("bigint")) * 60).cast("bigint").alias("dec_ss"),
    ((((((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60) - (((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60).cast("bigint")) * 60) - ((((((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60) - (((col("dec") / 15) - (col("dec") / 15).cast("bigint")) * 60).cast("bigint")) * 60).cast("bigint"))).alias("dec_f")
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
    df = df.drop("ra_hh", "ra_mm", "ra_ss", "ra_ff", "h", "dec_deg", "dec_mm", "dec_ss", "dec_f")
    
    return df


def aid_replacer(df):
    df = df.sort("mjd", ascending=True)
    df_unique_oid = df.dropDuplicates(["oid"]).select("oid", "aid").withColumnRenamed('aid', 'aid_replaced') 
    df = df.join(df_unique_oid, on=["oid"], how="left")
    df = df.withColumn("aid", df["aid_replaced"]).drop("aid_replaced") #! we are keeping one with column
    return df

############################### TESTING ##############################################

def run_sorting_hat_step(avro_parquets_dir):
    df = load_dataframes(avro_parquets_dir)
    df = df_sorting_hat(df)
    sorted_columns_df = sorted(df.columns)
    df = df.select(*sorted_columns_df)
    return df

#! Hay que guardar temporalmente estos archivos para poder pasarselos como input al prv_candidates de pyspark, pero no será neesario a futuro