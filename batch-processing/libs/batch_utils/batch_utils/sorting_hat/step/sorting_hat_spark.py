import math

from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, udf
import polars

def df_sorting_hat(df):
    df = _parse_ztf_df(df)
    return df

def df_with_aid_by_oid(df):
    sample = df.select(polars.col("oid")).collect()
    oids = [row[0] for row in sample.iter_rows()]

def id_generator(ra: float, dec: float) -> int:
    """
    Method that create an identifier of 19 digits given its ra, dec.
    :param ra: right ascension in degrees
    :param dec: declination in degrees
    :return: alerce id
    """
    # 19-Digit ID - two spare at the end for up to 100 duplicates
    aid = 1000000000000000000

    # 2013-11-15 KWS Altered code to fix the negative RA problem
    if ra < 0.0:
        ra += 360.0

    if ra > 360.0:
        ra -= 360.0

    # Calculation assumes Decimal Degrees:
    ra_hh = int(ra / 15)
    ra_mm = int((ra / 15 - ra_hh) * 60)
    ra_ss = int(((ra / 15 - ra_hh) * 60 - ra_mm) * 60)
    ra_ff = int((((ra / 15 - ra_hh) * 60 - ra_mm) * 60 - ra_ss) * 100)

    if dec >= 0:
        h = 1
    else:
        h = 0
        dec = dec * -1

    dec_deg = int(dec)
    dec_mm = int((dec - dec_deg) * 60)
    dec_ss = int(((dec - dec_deg) * 60 - dec_mm) * 60)
    dec_f = int(((((dec - dec_deg) * 60 - dec_mm) * 60) - dec_ss) * 10)

    aid += ra_hh * 10000000000000000
    aid += ra_mm * 100000000000000
    aid += ra_ss * 1000000000000
    aid += ra_ff * 10000000000

    aid += h * 1000000000
    aid += dec_deg * 10000000
    aid += dec_mm * 100000
    aid += dec_ss * 1000
    aid += dec_f * 100
    # transform to str
    return aid

def _parse_ztf_df(df):
    df = unnest_candidate(df)
    # df = df.withColumn("candidate", explode("candidate"))
    # columns_to_drop = [
    #     "schemavsn",
    #     "publisher",
    #     "candidate",
    # ]
    # for col in columns_to_drop:
    #     df = df.drop(col)


    ERRORS = {
        1: 0.065,
        2: 0.085,
        3: 0.01,
    }

    #! REVISAR, EL ERRORS de ARRIBA ES EL QUE ESTÁ EN EL SORTING HAT, PERO DA ERROR
    ERRORS = {
        "g": 0.065,
        "r": 0.085,
        "i": 0.01,
    }

    FILTER = {
        1: "g",
        2: "r",
        3: "i",
    }

    def _e_ra(dec, fid):
        try:
            return ERRORS[fid] / abs(math.cos(math.radians(dec)))
        except ZeroDivisionError:
            return float("nan")

    # Define UDFs for transformation functions
    def map_fid(fid):
        return FILTER.get(fid)

    def map_mjd(jd):
        return jd - 2400000.5

    def map_e_ra(dec, fid):
        return _e_ra(dec, fid)

    def map_e_dec(fid):
        return ERRORS.get(fid)

    def map_isdiffpos(value):
        return 1 if value in ["t", "1"] else -1

    # Register UDFs
    map_fid_udf = udf(map_fid, StringType())
    map_mjd_udf = udf(map_mjd, DoubleType())
    map_e_ra_udf = udf(map_e_ra, DoubleType())
    map_e_dec_udf = udf(map_e_dec, DoubleType())
    map_isdiffpos_udf = udf(map_isdiffpos, IntegerType())


    # Apply transformations to DataFrame
    parsed_df = df\
                .withColumn("candid", df.candid.cast('string'))\
                .withColumn("oid", df.oid.cast('string'))\
                .withColumn("tid", lit("ZTF"))\
                .withColumn("sid", lit("ZTF"))\
                .withColumn("fid", map_fid_udf("fid")) \
                .withColumn("mjd", map_mjd_udf("mjd")) \
                .withColumn("e_ra", map_e_ra_udf("dec",  "fid"))\
                .withColumn("e_dec", map_e_dec_udf("fid"))\
                .withColumn("isdiffpos", map_isdiffpos_udf("isdiffpos"))

    return parsed_df

def df_oid_ra_dec(df):
    df = df.selectExpr(
        "oid",
        "ra",
        "dec"
    )
    return df

def unnest_candidate(df):
    df = df.selectExpr(
        "objectId as oid",
        "prv_candidates",
        "cutoutScience",
        "cutoutTemplate",
        "cutoutDifference",
        "fp_hists",
        "candidate.jd as mjd",
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
        "candidate.magpsf as mag",
        "candidate.sigmapsf as e_mag",
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