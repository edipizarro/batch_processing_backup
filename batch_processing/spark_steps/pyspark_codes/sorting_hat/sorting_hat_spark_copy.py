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
    df = add_aids(df)
    return df

############################### SORTING HAT PARSING ##############################################

# We define the steps to parse the dataframe to the alert structure
def _parse_ztf_df(df):
    df = _drop_unnecessary_fields(df)
    df = _unnest_candidate(df)
    df = _rename_columns(df)
    df = _apply_transformations(df)
    df = _parse_extrafields(df)
    return df     

# The idea behind parquets_avros_only is having a flag to say if the aid replacement is only from other avros parquets, and if we develop a way to combine parquets of avros
# and the aid from dets/ndets/fphots to the result. This will allow to not have to keep always the avro parquets, but just the 3 dataframes
def add_aids(df, parquets_avros_only = True):
    df = alerce_id_generator(df)
    df = aid_replacer(df)
    return df

# We drop the data from the stamps
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


# We unnest the candidate. To do so we must drop the candid, since its contained in the candidate
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


def _rename_columns(df):
    df = df.withColumnRenamed('objectId','oid')\
           .withColumnRenamed('jd', 'mjd')\
           .withColumnRenamed('magpsf', 'mag')\
           .withColumnRenamed('sigmapsf', 'e_mag')
    return df


    #!Must be in same order as original when I add them? Apparently not, just remember to order the extra fields and main fields before returning

def _apply_transformations(df):
    df = df.withColumn("candid",col("candid").cast(StringType())) \
           .withColumn("oid",col("oid").cast(StringType())) 
    
    df = df.withColumn("tid", lit("ZTF"))\
           .withColumn("sid", lit("ZTF"))\
           .withColumn("unparsed_fid", col("fid"))\
           .withColumn("unparsed_isdiffpos", col("isdiffpos"))\
           .withColumn("unparsed_jd", col("mjd"))\
           .withColumn("fid", when(df["fid"] == 1, "g")
                             .when(df["fid"] == 2, "r")
                             .otherwise("i"))\
           .withColumn("mjd", col("mjd")- 2400000.5)\
           .withColumn("isdiffpos", when(df["isdiffpos"] == "t", 1)
                                   .when(df["isdiffpos"] == "1", 1)
                                   .otherwise(-1))
    
    df = df.withColumn("e_dec",  when(df["fid"] == "g", 0.065)
                                .when(df["fid"] == "r", 0.085)
                                .otherwise(0.01))
    
    condition = cos(radians(col("dec"))) != 0
    calculation = when(condition, col("e_dec") / abs(cos(radians(col("dec"))))).otherwise(float('nan'))
    df = df.withColumn("e_ra", calculation)


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
    columns_extra_fields.remove('oid')
    columns_extra_fields.remove('candid')
    
    df_extrafields = df_extrafields.withColumn("extra_fields", struct([col(c) for c in columns_extra_fields])).select("oid", "candid", "extra_fields")
    df = df.select(*not_extrafields)
    df = df.join(df_extrafields, on=["oid", "candid"])
    sorted_columns_df = sorted(df.columns)
    df = df.select(*sorted_columns_df)
    return df

############################### SORTING HAT AID GENERATION ##############################################

def alerce_id_generator(df):

    # Fix negative Ra
    df = df.withColumn("ra", F.when(F.col("ra") < 0, F.col("ra") + 360).otherwise(F.col("ra")))
    df = df.withColumn("ra", F.when(F.col("ra") > 360.0, F.col("ra") - 360).otherwise(F.col("ra")))

    # Calculation of aid natively
    df = df.withColumn("ra_hh", (F.col("ra") / 15).cast("bigint"))  
    df = df.withColumn("ra_mm", ((F.col("ra") / 15 - F.col("ra_hh")) * 60).cast("bigint"))
    df = df.withColumn("ra_ss", (((F.col("ra") / 15 - F.col("ra_hh")) * 60 - F.col("ra_mm")) * 60).cast("bigint"))
    df = df.withColumn("ra_ff", ((((F.col("ra") / 15 - F.col("ra_hh")) * 60 - F.col("ra_mm")) * 60 - F.col("ra_ss")) * 100).cast("bigint"))

    # Fix negative Dec
    df = df.withColumn("h", F.when(F.col("dec") >= 0, F.lit(1)).otherwise(F.lit(0)))
    df = df.withColumn("dec", F.abs(F.col("dec")))

    #Calculations to get aid natively
    df = df.withColumn("dec_deg", (F.col("dec")).cast("bigint"))
    df = df.withColumn("dec_mm", ((F.col("dec") - F.col("dec_deg")) * 60).cast("bigint"))
    df = df.withColumn("dec_ss", ((((F.col("dec") - F.col("dec_deg")) * 60) - F.col("dec_mm")) * 60).cast("bigint"))
    df = df.withColumn("dec_f", ((((F.col("dec") - F.col("dec_deg")) * 60) - F.col("dec_mm")) * 60 - F.col("dec_ss") * 10).cast("bigint"))

    # Calculating the aid
    df = df.withColumn("default_aid", F.lit(1000000000000000000))
    df = df.withColumn("aid", 
                   F.col("default_aid") +
                   (F.col("ra_hh") * 10000000000000000) +
                   (F.col("ra_mm") * 100000000000000) +
                   (F.col("ra_ss") * 1000000000000) +
                   (F.col("ra_ff") * 10000000000) +
                   (F.col("h") * 1000000000) +
                   (F.col("dec_deg") * 10000000) +
                   (F.col("dec_mm") * 100000) +
                   (F.col("dec_ss") * 1000) +
                   (F.col("dec_f") * 100)
                  )

    # Drop intermediate columns
    df = df.drop("ra_hh", "ra_mm", "ra_ss", "ra_ff", "h", "dec_deg", "dec_mm", "dec_ss", "dec_f", "default_aid")
    df = df.withColumn("aid",col("aid").cast(StringType()))
    return df

def aid_replacer(df):
    # Order by mjd to get the first appeareance of oid/aid pair
    df = df.sort("mjd", ascending=True)
    df_unique_oid = df.dropDuplicates(["oid"]).select("oid", "aid").withColumnRenamed('aid', 'aid_replaced') #Change name to distinguish two possible different cols
    df = df.join(df_unique_oid, on=["oid"], how="left")
    df = df.withColumn("aid", df["aid_replaced"]) # Swap the aid_replaced as the new aid value
    df = df.drop("aid_replaced") # Drop the repeated column
    return df


############################### TESTING ##############################################

def run_sorting_hat_step(avro_parquets_dir):
    df = load_dataframes(avro_parquets_dir)
    df = df_sorting_hat(df)
    df = add_aids(df)
    sorted_columns_df = sorted(df.columns)
    df = df.select(*sorted_columns_df)
    return df

#! Hay que guardar temporalmente estos archivos para poder pasarselos como input al prv_candidates de pyspark, pero no será neesario a futuro