import polars  
from polars import String, Int64, Float64, List, Struct, Boolean
import numpy as np
import math
from collections import OrderedDict
import threading

_ZERO_MAG = 100.0


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

schema_parquet = OrderedDict([('aid', Int64), ('candid', String), ('dec', Float64), ('e_dec', Float64), ('e_mag', Float64), ('e_ra', Float64), ('extra_fields', Struct({'aimage': Float64, 'aimagerat': Float64, 'bimage': Float64, 'bimagerat': Float64, 'chinr': Float64, 'chipsf': Float64, 'classtar': Float64, 'clrcoeff': Float64, 'clrcounc': Float64, 'clrmed': Float64, 'clrrms': Float64, 'decnr': Float64, 'diffmaglim': Float64, 'distnr': Float64, 'distpsnr1': Float64, 'distpsnr2': Float64, 'distpsnr3': Float64, 'drb': Float64, 'drbversion': String, 'dsdiff': Float64, 'dsnrms': Float64, 'elong': Float64, 'exptime': Float64, 'field': Int64, 'fwhm': Float64, 'jdendhist': Float64, 'jdendref': Float64, 'jdstarthist': Float64, 'jdstartref': Float64, 'magap': Float64, 'magapbig': Float64, 'magdiff': Float64, 'magfromlim': Float64, 'maggaia': Float64, 'maggaiabright': Float64, 'magnr': Float64, 'magzpsci': Float64, 'magzpscirms': Float64, 'magzpsciunc': Float64, 'mindtoedge': Float64, 'nbad': Int64, 'ncovhist': Int64, 'ndethist': Int64, 'neargaia': Float64, 'neargaiabright': Float64, 'nframesref': Int64, 'nid': Int64, 'nmatches': Int64, 'nmtchps': Int64, 'nneg': Int64, 'objectidps1': Int64, 'objectidps2': Int64, 'objectidps3': Int64, 'pdiffimfilename': String, 'programid': Int64, 'programpi': String, 'ranr': Float64, 'rb': Float64, 'rbversion': String, 'rcid': Int64, 'rfid': Int64, 'scorr': Float64, 'seeratio': Float64, 'sgmag1': Float64, 'sgmag2': Float64, 'sgmag3': Float64, 'sgscore1': Float64, 'sgscore2': Float64, 'sgscore3': Float64, 'sharpnr': Float64, 'sigmagap': Float64, 'sigmagapbig': Float64, 'sigmagnr': Float64, 'simag1': Float64, 'simag2': Float64, 'simag3': Float64, 'sky': Float64, 'srmag1': Float64, 'srmag2': Float64, 'srmag3': Float64, 'ssdistnr': Float64, 'ssmagnr': Float64, 'ssnamenr': String, 'ssnrms': Float64, 'sumrat': Float64, 'szmag1': Float64, 'szmag2': Float64, 'szmag3': Float64, 'tblid': Int64, 'tooflag': Int64, 'xpos': Float64, 'ypos': Float64, 'zpclrcov': Float64, 'zpmed': Float64})), ('fid', String), ('isdiffpos', Int64), ('mag', Float64), ('mjd', Float64), ('oid', String), ('pid', Int64), ('ra', Float64), ('sid', String), ('tid', String), ('unparsed_fid', Int64), ('unparsed_isdiffpos', String), ('unparsed_jd', Float64), ('detections', List(Struct({'aid': Int64, 'candid': String, 'dec': Float64, 'e_dec': Float64, 'e_mag': Float64, 'e_ra': Float64, 'extra_fields': Struct({'aimage': Float64, 'aimagerat': Float64, 'bimage': Float64, 'bimagerat': Float64, 'chinr': Float64, 'chipsf': Float64, 'classtar': Float64, 'clrcoeff': Float64, 'clrcounc': Float64, 'decnr': Float64, 'diffmaglim': Float64, 'distnr': Float64, 'elong': Float64, 'field': Int64, 'fwhm': Float64, 'magap': Float64, 'magapbig': Float64, 'magdiff': Float64, 'magfromlim': Float64, 'magnr': Float64, 'magzpsci': Float64, 'magzpscirms': Float64, 'magzpsciunc': Float64, 'mindtoedge': Float64, 'nbad': Int64, 'nid': Int64, 'nneg': Int64, 'pdiffimfilename': String, 'programid': Int64, 'programpi': String, 'ranr': Float64, 'rb': Float64, 'rbversion': String, 'rcid': Int64, 'scorr': Float64, 'seeratio': Float64, 'sharpnr': Float64, 'sigmagap': Float64, 'sigmagapbig': Float64, 'sigmagnr': Float64, 'sky': Float64, 'ssdistnr': Float64, 'ssmagnr': Float64, 'ssnamenr': String, 'sumrat': Float64, 'tblid': Int64, 'xpos': Float64, 'ypos': Float64}), 'fid': String, 'forced': Boolean, 'has_stamp': Boolean, 'isdiffpos': Int64, 'mag': Float64, 'mjd': Float64, 'oid': String, 'parent_candid': String, 'pid': Int64, 'ra': Float64, 'sid': String, 'tid': String, 'unparsed_fid': Int64, 'unparsed_isdiffpos': String, 'unparsed_jd': Float64}))), ('non_detections', List(Struct({'diffmaglim': Float64, 'fid': String, 'mjd': Float64, 'oid': String, 'sid': String, 'tid': String, 'unparsed_fid': Int64, 'unparsed_jd': Float64, 'aid': Int64}))), ('forced_photometries', List(Struct({'aid': Int64, 'candid': String, 'dec': Float64, 'e_dec': Int64, 'e_mag': Float64, 'e_ra': Int64, 'extra_fields': Struct({'adpctdif1': Float64, 'adpctdif2': Float64, 'chinr': Float64, 'clrcoeff': Float64, 'clrcounc': Float64, 'decnr': Float64, 'diffmaglim': Float64, 'distnr': Float64, 'exptime': Float64, 'field': Int64, 'forcediffimflux': Float64, 'forcediffimfluxunc': Float64, 'magnr': Float64, 'magzpsci': Float64, 'magzpscirms': Float64, 'magzpsciunc': Float64, 'procstatus': String, 'programid': Int64, 'ranr': Float64, 'rcid': Int64, 'rfid': Int64, 'scibckgnd': Float64, 'sciinpseeing': Float64, 'scisigpix': Float64, 'sharpnr': Float64, 'sigmagnr': Float64}), 'fid': String, 'forced': Boolean, 'has_stamp': Boolean, 'isdiffpos': Int64, 'mag': Float64, 'mjd': Float64, 'oid': String, 'parent_candid': String, 'pid': Int64, 'ra': Float64, 'sid': String, 'tid': String, 'unparsed_fid': Int64, 'unparsed_jd': Float64})))])

#! Transformar a logica de la api de polars es posible?
def _e_ra(dec_fid):
        dec, fid = dec_fid
        try:
            return ERRORS[fid] / abs(math.cos(math.radians(dec)))
        except ZeroDivisionError:
            return float("nan")

# Define functions for transformation
def map_fid(fid):
    return FILTER.get(fid)

def map_e_ra(dec_fid):
    return _e_ra(dec_fid)

def parser_detections_and_fp(df_candidate, type_of_detection):
    df_candidate = rename_columns(df_candidate, type_of_detection)
    df_candidate = apply_transformations(df_candidate, type_of_detection)
    df_candidate = parse_extra_fields(df_candidate, type_of_detection)
    return df_candidate


def rename_columns(df, type_of_detection):
    if type_of_detection == "previous_detection" or type_of_detection == "forced_photometries":
        to_rename = [
        {"objectId": "oid"},
        {"jd": "mjd"},
        {"magpsf": "mag"},
        {"sigmapsf": "e_mag"},
        ]
    elif type_of_detection == "non_detection":
        to_rename = [
        {"objectId": "oid"},
        {"jd": "mjd"},
        ]
    for rename_map in to_rename:
            df = df.rename(rename_map)
    return df


### apply transformations can be combined better, however it is easier to understand the respective transformations as separate
def apply_transformations(df, type_of_detection):

    if type_of_detection == "previous_detection":   
        df = df.cast({
        "candid": polars.String,
        "oid": polars.String,
        })

        e_ra = df.select(["dec", "fid"]).apply(map_e_ra).rename({"map": "e_ra"})
        

        df = df.with_columns(polars.lit("ZTF").alias("tid"))\
            .with_columns(e_ra)\
            .with_columns(polars.lit("ZTF").alias("sid"))\
            .with_columns(polars.when(polars.col("fid")==1).then(polars.lit("g"))
                          .when(polars.col("fid")==2).then(polars.lit("r"))
                          .otherwise(polars.lit("i")).alias("fid"))\
            .with_columns(df["fid"].alias("unparsed_fid"))\
            .with_columns((df["mjd"] - 2400000.5).alias("mjd"))\
            .with_columns((df["mjd"]).alias("unparsed_jd"))\
            .with_columns(polars.when(polars.col("fid")=="g").then(0.065)
                          .when(polars.col("fid")=="r").then(0.085)
                          .otherwise(0.01).alias("e_dec"))\
            .with_columns(polars.when(polars.col("isdiffpos")=="1").then(1)
                                      .when(polars.col("isdiffpos")=="t").then(1)
                                      .otherwise(-1).alias("isdiffpos"))\
            .with_columns(df['isdiffpos'].alias("unparsed_isdiffpos"))
        



    if type_of_detection == "forced_photometries":
        df = df.cast({
        "candid": polars.String,
        "oid": polars.String,
        })
        
        df = df.with_columns(polars.lit("ZTF").alias("tid"))\
            .with_columns(polars.lit("ZTF").alias("sid"))\
            .with_columns(polars.when(polars.col("fid")==1).then(polars.lit("g"))
                          .when(polars.col("fid")==2).then(polars.lit("r"))
                          .otherwise(polars.lit("i")).alias("fid"))\
            .with_columns(df["fid"].alias("unparsed_fid"))\
            .with_columns((df["mjd"] - 2400000.5).alias("mjd"))\
            .with_columns((df["mjd"]).alias("unparsed_jd"))\
            .with_columns(polars.lit(0).alias("e_dec"))\
            .with_columns(polars.lit(0).alias("e_ra"))\
            .with_columns(polars.when(polars.col("forcediffimflux")>=0).then(1)
                          .otherwise(-1).alias("isdiffpos"))        


    if type_of_detection == "non_detection":
        df = df.cast({
        "oid": polars.String,
        })

        df = df.with_columns(polars.lit("ZTF").alias("tid"))\
            .with_columns(polars.lit("ZTF").alias("sid"))\
            .with_columns(polars.when(polars.col("fid")==1).then(polars.lit("g"))
                          .when(polars.col("fid")==2).then(polars.lit("r"))
                          .otherwise(polars.lit("i")).alias("fid"))\
            .with_columns(df["fid"].alias("unparsed_fid"))\
            .with_columns((df["mjd"] - 2400000.5).alias("mjd"))\
            .with_columns((df["mjd"]).alias("unparsed_jd"))\
            .with_columns(polars.lit(df['diffmaglim']).alias('diffmaglim'))
        
    return df

# Columns not here go to extra fields
def parse_extra_fields(df, type_of_detection):
    if type_of_detection=="previous_detection":
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
    if type_of_detection=="forced_photometries":
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
        "unparsed_fid"
        ]
    if type_of_detection=="non_detection":
        not_extrafields = [
        "oid",
        "tid",
        "sid",
        "mjd",
        "fid",
        "unparsed_jd",
        "unparsed_fid",
        "diffmaglim"
        ]
    #! Cast nulls from detections to empty string/and -999.0 => Solve pyspark complication when reading parquets. Specific to these columns
    # For a sample of data, dropping them also solved the issue. More may arise similar to this one
    if type_of_detection=="previous_detection":
        replaced_ssnamenr =  polars.DataFrame(df['ssnamenr'].fill_null(''))
        replaced_ssdistnr=  polars.DataFrame(df['ssdistnr'].fill_null(-999.0))
        replaced_ssmagnr =  polars.DataFrame(df['ssmagnr'].fill_null(-999.0))
        df = df.with_columns(replaced_ssnamenr)
        df = df.with_columns(replaced_ssdistnr)
        df = df.with_columns(replaced_ssmagnr)
    df_extrafields = df.drop(not_extrafields)
    df_extrafields = df_extrafields.select(sorted(df_extrafields.columns))
    column_extrafields = polars.DataFrame({"extra_fields": df_extrafields})
    df = df.select(not_extrafields)
    df = df.with_columns(column_extrafields)
    df = df.select(sorted(df.columns))
    return df

def value_column(df, column_name):
    return df.select(column_name).item()


def parse_message_detections(prv_candidates_detections_list, df_alert):
    oid =  value_column(df_alert, "oid")  
    prv_candidates_detections_list = prv_candidates_detections_list.with_columns(polars.lit(oid).alias('objectId'))
    detection_list_parsed =  parser_detections_and_fp(prv_candidates_detections_list, "previous_detection")
    aid = value_column(df_alert, "aid")
    candid = value_column(df_alert, "candid")
    detection_list_parsed = detection_list_parsed.with_columns(polars.lit(aid).alias("aid"))\
                    .with_columns(polars.lit(False).alias("has_stamp"))\
                    .with_columns(polars.lit(False).alias("forced"))\
                    .with_columns(polars.lit(candid).alias("parent_candid"))
    detection_list_parsed = detection_list_parsed.select(sorted(detection_list_parsed.columns))
    return detection_list_parsed

def parse_message_non_detections(prv_candidates_nondetections_list, df_alert):
    oid =  value_column(df_alert, "oid")  
    prv_candidates_nondetections_list = prv_candidates_nondetections_list.with_columns(polars.lit(oid).alias('objectId'))
    non_detection_list_parsed =  parser_detections_and_fp(prv_candidates_nondetections_list, "non_detection")
    non_detection_list_parsed = non_detection_list_parsed.drop('extra_fields') ## non_detections extra fields are dropped 
    aid = value_column(df_alert, "aid") # non_detections adds the aid from the alert
    parsed_non_detection = non_detection_list_parsed.with_columns(polars.lit(aid).alias("aid"))
    return parsed_non_detection

def calculate_mag(fp_data):
        magzpsci = fp_data.select("magzpsci").to_numpy()  
        flux2uJy = 10.0 ** ((8.9 - magzpsci) / 2.5) * 1.0e6
        forcediffimflux = fp_data.select("forcediffimflux").to_numpy()
        forcediffimflux = forcediffimflux * flux2uJy        
        forcediffimfluxunc = fp_data.select("forcediffimfluxunc").to_numpy()
        forcediffimfluxunc = forcediffimfluxunc * flux2uJy
        mag = np.array(-2.5 * np.log10(np.abs(forcediffimflux)) + 23.9)
        e_mag = np.array(1.0857 * forcediffimfluxunc / np.abs(forcediffimflux))
        mask_forcediffimflux = np.isclose(forcediffimflux, -99999)
        mask_forcediffimfluxunc = np.isclose(forcediffimfluxunc, -99999)
        mag[mask_forcediffimflux] = _ZERO_MAG
        e_mag[mask_forcediffimflux] = _ZERO_MAG
        e_mag[mask_forcediffimfluxunc] = _ZERO_MAG
        return mag, e_mag
    
def parse_message_forced_photometry(forced_photometry, df_alert):
    oid = df_alert.select("oid").to_numpy().flatten()
    pid = forced_photometry.select('pid').to_numpy()
    new_candid = np.array([oid[0] + str(val[0]) for val in pid]).flatten()
    magpsf, sigmaspf = calculate_mag(forced_photometry)
    objectId = value_column(df_alert, "oid")
    ra = value_column(df_alert, "ra")
    dec = value_column(df_alert, "dec")
    forced_photometry = forced_photometry.with_columns(polars.lit(new_candid).alias("candid"))\
                                        .with_columns(polars.lit(magpsf.flatten()).alias("magpsf"))\
                                        .with_columns(polars.lit(sigmaspf.flatten()).alias("sigmapsf"))\
                                        .with_columns(polars.lit(objectId).alias("objectId"))\
                                        .with_columns(polars.lit(ra).alias("ra"))\
                                        .with_columns(polars.lit(dec).alias("dec"))
    parsed_photometry = parser_detections_and_fp(forced_photometry, "forced_photometries")
    aid = value_column(df_alert, "aid")
    candid = value_column(df_alert, "candid")
    parsed_photometry = parsed_photometry.with_columns(polars.lit(aid).alias("aid"))\
                .with_columns(polars.lit(False).alias("has_stamp"))\
                .with_columns(polars.lit(True).alias("forced"))\
                .with_columns(polars.lit(candid).alias("parent_candid"))
    parsed_photometry = parsed_photometry.select(sorted(parsed_photometry.columns))
    return parsed_photometry
    
       
def extract_detections_and_non_detections(df_alert) -> polars.DataFrame:
    non_detections = polars.DataFrame()   # Handle no prv_candidates for (non)detections and fp_hists
    forced_photometries =  polars.DataFrame()  
    detections = polars.DataFrame()

    extrafields_df_alert = df_alert.select("extra_fields").unnest("extra_fields")
    previous_candidates_null_condition = extrafields_df_alert.select('prv_candidates').item() # no prv_candidates condition
    if previous_candidates_null_condition is not None:
        candidates_list = extrafields_df_alert.select('prv_candidates').explode('prv_candidates').unnest('prv_candidates')
        detections_list_toparse = candidates_list.filter(candidates_list['candid'].is_not_null())
        non_detections_list_toparse = candidates_list.filter(candidates_list['candid'].is_null())
        ### Handle nulls in detections to parse after grouping by candid
        if len(detections_list_toparse) > 0:
            detections = parse_message_detections(detections_list_toparse, df_alert)
        ### Handle nulls in non-detections to parse after grouping by candid
        if len(non_detections_list_toparse) > 0:
            non_detections = parse_message_non_detections(non_detections_list_toparse, df_alert)

    forced_photometries_null_condition = extrafields_df_alert.select('fp_hists').item() # no fp_hists condition
    if forced_photometries_null_condition is not None:
        forced_photometries_list = extrafields_df_alert.select('fp_hists').explode('fp_hists').unnest('fp_hists')
        forced_photometries = parse_message_forced_photometry(forced_photometries_list, df_alert)        

    # Drop the prv_candidates and fp_hists columns after parsing the data to outer columns
    extrafields_df_alert = extrafields_df_alert.drop("prv_candidates").drop("fp_hists")
    updated_extra_fields = polars.DataFrame({"extra_fields": extrafields_df_alert})
    df_alert = df_alert.with_columns(updated_extra_fields)
    
    #convert all detections/photometries to list of dicts
    dict_list_detections = detections.to_dicts()
    dict_list_non_detections = non_detections.to_dicts()
    dict_list_forced_photometries = forced_photometries.to_dicts()
    
    #create a dataframe of a single cell for each dict_list
    
    detection_list = polars.DataFrame({"detections" : [dict_list_detections]})
    non_detection_list = polars.DataFrame({"non_detections" : [dict_list_non_detections]})
    forced_photometries_list = polars.DataFrame({"forced_photometries" : [dict_list_forced_photometries]})
    
    # get oid and aid values
    oid = polars.DataFrame({"oid" : value_column(df_alert, "oid")})
    aid = polars.DataFrame({"aid" : value_column(df_alert, "aid")})

    # generate output frame 
    df_output = df_alert.with_columns(oid)\
                                .with_columns(aid)\
                                .with_columns(detection_list)\
                                .with_columns(non_detection_list)\
                                .with_columns(forced_photometries_list)
    return df_output


# Cycling through each of the dataframe alerts (modify for an apply instead and merge all resulting dataframes)
# Concat uses diagonal relaxed to handle possible null frames in detections, non-detections and forced photometries
def extract_detections_and_non_detections_dataframe(df):
    dataframe_updated = polars.DataFrame(schema = schema_parquet)
    for row in range(len(df)):
        alert_updated = extract_detections_and_non_detections(df[row])
        dataframe_updated = polars.concat([dataframe_updated, alert_updated], how = "diagonal_relaxed")
    return dataframe_updated



# Divide the original frame in subframes. Returns an array with each subframe inside it
def divide_in_subframes(df, num_processes):
    slice_size = int(len(df) / num_processes)
    array_dataframe_chunks = []
    for slices in df.iter_slices(slice_size):
        array_dataframe_chunks.append(slices)
    return array_dataframe_chunks
    
# Multiprocessing to accelerate prv candidates processing 
# There's a better option to optimize prv_candidates => explode all necessary rows/cols and work the whole file in one go 
# It's more complex to program
def extract_detections_and_non_detections_dataframe_multiprocessing(df, num_processes):
    df_chunks = divide_in_subframes(df, num_processes)
    threads = []
    processed_sections = []

    def process_section_thread(section):
        processed_sections.append(extract_detections_and_non_detections_dataframe(section))
    for section in df_chunks:
        thread = threading.Thread(target=process_section_thread, args=(section,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
        
    processed_df = polars.concat(processed_sections, how = "diagonal_relaxed")
    return processed_df





