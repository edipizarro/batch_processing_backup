import math
import polars

from batch_processing.db_reader import DBParquetReader

#! Sorting Hat to use when keeping old AIDs
# def df_sorting_hat(db_reader: DBParquetReader, df: polars.DataFrame):
#     df = _parse_ztf_df(df)
#     df = df_with_aid_column_matched_by_oid_in_parquet_db_object_collection(db_reader, df)
#     # Conesearch
#     # Generate ID
#     return df

#! Sorting hat without looking for AID on db
def df_sorting_hat(db_reader: DBParquetReader, df: polars.DataFrame):
    df = _parse_ztf_df(df)
    df = add_new_aid(df)
    return df


def sorting_hat_aid_replacer(db_reader: DBParquetReader, df: polars.DataFrame, lazy_oid_aid_all_data: polars.LazyFrame | None):
    if lazy_oid_aid_all_data is not None: # If there are prev parquets
        lazy_df_unique_oid = lazy_oid_aid_all_data.unique(subset=['oid'], keep='first') # Find first appearance of an oid
        joined_df = df.join(lazy_df_unique_oid, on='oid', how='left') # Find aid for each oid of the current df
        # Coalesce, aid_right corresponds to the aid to replace. aid corresponds to the aid in the current df
        joined_df = joined_df.with_columns(polars.coalesce(["aid_right",  "aid"]). alias('aid'))
        joined_df = joined_df.drop('aid_right')
        df = joined_df
        df = df.select(sorted(df.columns))
    return df

def df_with_aid_column_matched_by_oid_in_parquet_db_object_collection(db_reader: DBParquetReader, df: polars.DataFrame):
    oid_df = df.select(polars.col("oid"))
    oids = [row[0] for row in oid_df.iter_rows()]
    df_matched_columns = db_reader.df_matched_column("oid", oids, ["oid", "aid"])
    df.join(df_matched_columns, on="oid", how="left")
    return df

# def cone_search():
        ## si no, revisar si ya existe aid para el object (busqueda por cone search?)
                #     db.database["object"].find_one(
                #     {
                #         "loc": {
                #             "$nearSphere": {
                #                 "$geometry": {
                #                     "type": "Point",
                #                     "coordinates": [ra - 180, dec],
                #                 },
                #                 "$maxDistance": math.radians(radius / 3600) * 6.3781e6,
                #             },
                #         },
                #     },
                #     {"aid": 1},
                # )

def add_new_aid(df: polars.DataFrame):
    aid = create_aid_df_column(df)
    df = df.with_columns(aid)
    return df

def create_aid_df_column(df):
    oid_to_aid = {}
    aid_list = []
    columns = ["oid", "ra", "dec", "aid"] if "aid" in df.columns else ["oid", "ra", "dec"]
    for row in df.select(columns).iter_rows():
        if "aid" in columns:
            oid, ra, dec, aid = row
        else:
            oid, ra, dec, aid = [*row, None]

        if aid:
            oid_to_aid[oid] = aid
        else:
            aid = oid_to_aid.get(oid, None)
        if not aid:
            aid = alerce_id_generator(ra, dec)
            oid_to_aid[oid] = aid
        aid_list.append(aid)
    aid_series = polars.Series(name="aid", values=aid_list)
    return aid_series


def map_alerce_id_generator(ra_dec: tuple[float]):
    ra, dec = ra_dec
    return alerce_id_generator(ra, dec)

def alerce_id_generator(ra: float, dec: float) -> int:
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

def _parse_ztf_df(df: polars.DataFrame):
    df = _drop_unnecessary_fields(df)
    df = _unnest_candidate(df)
    df = _rename_columns(df)
    df = _apply_transformations(df)
    df = _parse_extrafields(df)
    return df     

def _drop_unnecessary_fields(df: polars.DataFrame):
    fields_to_drop = [
        "schemavsn",
        "publisher",
        "cutoutScience",
        "cutoutTemplate",
        "cutoutDifference"
    ]
    df = df.drop(fields_to_drop)
    return df


def _unnest_candidate(df: polars.DataFrame):
    df = df.drop("candid")
    df = df.unnest("candidate")
    return df

def _rename_columns(df: polars.DataFrame):
    to_rename = [
        {"objectId": "oid"},
        {"jd": "mjd"},
        {"magpsf": "mag"},
        {"sigmapsf": "e_mag"},
    ]
    for rename_map in to_rename:
        df = df.rename(rename_map)
    return df

def _parse_extrafields(df: polars.DataFrame):
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
    df_extrafields = df.drop(not_extrafields)
    df_extrafields = df_extrafields.select(sorted(df_extrafields.columns))
    column_extrafields = polars.DataFrame({"extra_fields": df_extrafields})
    df = df.select(not_extrafields)
    df = df.with_columns(column_extrafields)
    df = df.select(sorted(df.columns))
    return df

def _apply_transformations(df: polars.DataFrame):

    ERRORS = {
        1: 0.065,
        2: 0.085,
        3: 0.01,
    }

    FILTER = {
        1: "g",
        2: "r",
        3: "i",
    }

    def _e_ra(dec_fid):
        dec, fid = dec_fid
        try:
            return ERRORS[fid] / abs(math.cos(math.radians(dec)))
        except ZeroDivisionError:
            return float("nan")

    # Define functions for transformation
    def map_fid(fid):
        return FILTER.get(fid)

    def map_mjd(df_mjd):
        return df_mjd - 2400000.5

    def map_e_ra(dec_fid):
        return _e_ra(dec_fid)

    def map_e_dec(fid):
        return ERRORS.get(fid)

    def map_isdiffpos(value):
        return 1 if value in ["t", "1"] else -1

    # Apply transformations to DataFrame
    df = df.cast({
        "candid": polars.String,
        "oid": polars.String,
    })

    # .replace("e_ra", df.select(["dec", "fid"]).apply(map_e_ra))
    df = polars.concat([
            df,
            df.select(["dec", "fid"]).apply(map_e_ra).rename({"map": "e_ra"})
        ],
        how="horizontal"
    )    

    df = df.with_columns(polars.lit("ZTF").alias("tid"))\
        .with_columns(polars.lit("ZTF").alias("sid"))\
        .replace("fid", df["fid"].apply(map_fid))\
        .with_columns(df["fid"].alias("unparsed_fid"))\
        .replace("mjd", map_mjd(df["mjd"]))\
        .with_columns(df["mjd"].alias("unparsed_jd"))\
        .with_columns(df["fid"].apply(map_e_dec).alias("e_dec"))\
        .replace("isdiffpos", df["isdiffpos"].apply(map_isdiffpos))\
        .with_columns(df['isdiffpos'].alias("unparsed_isdiffpos"))

    return df
