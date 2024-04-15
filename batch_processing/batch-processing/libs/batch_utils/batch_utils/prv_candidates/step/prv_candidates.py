import polars  
import numpy as np
import math


### Define functions to map values in the parsers
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

def map_isdiffpos_previous_detection(value):
    return 1 if value in ["t", "1"] else -1

def map_isdiffpos_forced_photometries(value):
    return 1 if value >= 0 else -1

# Receives a dataframe and a string to distinguish the type of parser needed. Applies renaming, 
# parsing, and transformations, according to each parser, returning the parsed polars dataframe
def parser_detections_and_fp(df_candidate: polars.DataFrame, type_of_detection: str):
    df_candidate = rename_columns(df_candidate, type_of_detection)
    df_candidate = apply_transformations(df_candidate, type_of_detection)
    df_candidate = parse_extra_fields(df_candidate, type_of_detection)
    return df_candidate

# Receives a dataframe and a string to distinguish the type of parser needed. 
# Applies renaming of columns according to the type of parser used
def rename_columns(df: polars.DataFrame, type_of_detection: str):
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


# Receives dataframe and string to distinguish the type of parser needed. 
# Applies transformations to the dataframe
def apply_transformations(df: polars.DataFrame, type_of_detection: str):

    if type_of_detection == "previous_detection":   
        df = df.cast({
        "candid": polars.String,
        "oid": polars.String,
        })

        df = polars.concat([
            df,
            df.select(["dec", "fid"]).apply(map_e_ra).rename({"map": "e_ra"})
        ],
        how="horizontal"
        )
        
        isdiffpos = map_isdiffpos_previous_detection(df["isdiffpos"][0])


        df = df.with_columns(polars.lit("ZTF").alias("tid"))\
            .with_columns(polars.lit("ZTF").alias("sid"))\
            .with_columns(df["fid"].map_elements(map_fid).alias("fid"))\
            .with_columns(df["mjd"].map_elements(map_mjd).alias("mjd"))\
            .with_columns(df["fid"].map_elements(map_e_dec).alias("e_dec"))\
            .with_columns(polars.lit(isdiffpos).alias('isdiffpos'))


    if type_of_detection == "forced_photometries":
        df = df.cast({
        "candid": polars.String,
        "oid": polars.String,
        })
        
        isdiffpos = map_isdiffpos_forced_photometries(df["forcediffimflux"][0])

        df = df.with_columns(polars.lit("ZTF").alias("tid"))\
            .with_columns(polars.lit("ZTF").alias("sid"))\
            .with_columns(df["fid"].map_elements(map_fid).alias("fid"))\
            .with_columns(df["mjd"].map_elements(map_mjd).alias("mjd"))\
            .with_columns(polars.lit(0).alias("e_dec"))\
            .with_columns(polars.lit(0).alias("e_ra"))\
            .with_columns(polars.lit(isdiffpos).alias('isdiffpos'))
        

    if type_of_detection == "non_detection":
        df = df.cast({
        "oid": polars.String,
        })

        df = df.with_columns(polars.lit("ZTF").alias("tid"))\
            .with_columns(polars.lit("ZTF").alias("sid"))\
            .with_columns(df["fid"].map_elements(map_fid).alias("fid"))\
            .with_columns(df["mjd"].map_elements(map_mjd).alias("mjd"))\
            .with_columns(polars.lit(df['diffmaglim']).alias('diffmaglim'))
        
    return df

# Receives dataframe and string to distinguish the type of parser needed.
# Parses all the extra fields of the dataframe according to the parser used
def parse_extra_fields(df: polars.DataFrame, type_of_detection: str):
    if type_of_detection=="previous_detection" or type_of_detection=="forced_photometries":
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
        ]
    if type_of_detection=="non_detection":
        not_extrafields = [
        "oid",
        "tid",
        "sid",
        "mjd",
        "fid",
        "diffmaglim"
        ]
    df_extrafields = df.drop(not_extrafields)
    column_extrafields = polars.DataFrame({"extra_fields": df_extrafields})
    df = df.select(not_extrafields)
    df = df.with_columns(column_extrafields)
    return df


# Extract data from a column in a polars dataframe using item
def value_column(df: polars.DataFrame, column_name: str):
    return df.select(column_name).item()


class ZTFPreviousDetectionsParser():

    @classmethod
    def can_parse(cls, prev_candidate) -> bool:
        return prev_candidate['candid'].item() != None

    @classmethod
    def parse_message(self,prev_candidate, df_alert) -> dict:
        oid =  value_column(df_alert, "oid")  
        prev_candidate = prev_candidate.with_columns(polars.lit(oid).alias('objectId'))
        detection =  parser_detections_and_fp(prev_candidate, "previous_detection")
        aid = value_column(df_alert, "aid")
        candid = value_column(df_alert, "candid")
        detection = detection.with_columns(polars.lit(aid).alias("aid"))\
                    .with_columns(polars.lit(False).alias("has_stamp"))\
                    .with_columns(polars.lit(False).alias("forced"))\
                    .with_columns(polars.lit(candid).alias("parent_candid"))
        return detection
  



class ZTFForcedPhotometryParser():
    @classmethod
    def __calculate_mag(self, fp_data):
        magzpsci = value_column(fp_data, "magzpsci")  
        flux2uJy = 10.0 ** ((8.9 - magzpsci) / 2.5) * 1.0e6
        forcediffimflux = value_column(fp_data, "forcediffimflux") 
        forcediffimflux = forcediffimflux * flux2uJy
        forcediffimfluxunc = value_column(fp_data, "forcediffimfluxunc") 
        forcediffimfluxunc = forcediffimfluxunc * flux2uJy
        mag = -2.5 * np.log10(np.abs(forcediffimflux)) + 23.9
        e_mag = 1.0857 * forcediffimfluxunc / np.abs(forcediffimflux)
        if np.isclose(value_column(fp_data, "forcediffimflux"), -99999):
            mag = _ZERO_MAG
            e_mag = _ZERO_MAG
        if np.isclose(value_column(fp_data, "forcediffimfluxunc"), -99999):
            e_mag = _ZERO_MAG
        return mag, e_mag
    
    @classmethod
    def can_parse(self, forced_photometry) -> bool:
        return True

    @classmethod
    def parse_message(self, forced_photometry: polars.DataFrame, df_alert) -> dict:
        candid = value_column(df_alert, "candid") + str(forced_photometry["pid"].item())
        magpsf, sigmaspf = self.__calculate_mag(forced_photometry)
        objectId = value_column(df_alert, "oid")
        ra = value_column(df_alert, "ra")
        dec = value_column(df_alert, "dec")
        forced_photometry = forced_photometry.with_columns(polars.lit(candid).alias("candid"))\
                                             .with_columns(polars.lit(magpsf).alias("magpsf"))\
                                             .with_columns(polars.lit(sigmaspf).alias("sigmapsf"))\
                                             .with_columns(polars.lit(objectId).alias("objectId"))\
                                             .with_columns(polars.lit(ra).alias("ra"))\
                                             .with_columns(polars.lit(dec).alias("dec"))

        parsed_photometry = parser_detections_and_fp(forced_photometry, "forced_photometries")
        aid = value_column(df_alert, "aid")
        candid = value_column(df_alert, "candid")
        parsed_photometry = parsed_photometry.with_columns(polars.lit(aid).alias("aid"))\
                    .with_columns(polars.lit(False).alias("has_stamp"))\
                    .with_columns(polars.lit(False).alias("forced"))\
                    .with_columns(polars.lit(candid).alias("parent_candid"))
        return parsed_photometry

    
       

class ZTFNonDetectionsParser():

    @classmethod
    def can_parse(cls, prev_candidate) -> bool:
        return prev_candidate["candid"].item() == None

    @classmethod
    def parse_message(self, prev_candidate, df_alert) -> dict:
        oid =  value_column(df_alert, "oid")  
        prev_candidate = prev_candidate.with_columns(polars.lit(oid).alias('objectId'))
        non_detection =  parser_detections_and_fp(prev_candidate, "non_detection")
        non_detection = non_detection.drop('extra_fields') ## non_detections drop extra fields
        aid = value_column(df_alert, "aid") # non_detections adds the aid from the alert
        parsed_non_detection = non_detection.with_columns(polars.lit(aid).alias("aid"))
        return parsed_non_detection
    

# Receives the dataframe of a single alert, and returns the same dataframe with the detection, non-detections and forced photometries
# parsed from the extra fields as columns
def extract_detections_and_non_detections(df_alert: polars.DataFrame) -> dict:
    extrafields_df_alert = df_alert.select("extra_fields").unnest("extra_fields")
    extra_fields_columns = extrafields_df_alert.columns
    non_detections = polars.DataFrame()
    detections = polars.DataFrame()               
    forced_photometries = polars.DataFrame()      
    
    previous_candidates_null_condition = extrafields_df_alert.select('prv_candidates').item() # no prv_candidates condition
    if previous_candidates_null_condition != None:
        candidates_list = extrafields_df_alert.select('prv_candidates')
        prv_candidates = candidates_list.explode('prv_candidates').unnest('prv_candidates')
    
        for row in range(len(prv_candidates)):
            if ZTFPreviousDetectionsParser.can_parse(prv_candidates[row]):
                parsed_prv_detection = ZTFPreviousDetectionsParser.parse_message(prv_candidates[row], df_alert)
                detections = polars.concat([detections, parsed_prv_detection])
            if ZTFNonDetectionsParser.can_parse(prv_candidates[row]):
                parsed_non_detection = ZTFNonDetectionsParser.parse_message(prv_candidates[row], df_alert)
                non_detections = polars.concat([non_detections, parsed_non_detection])
                
    #! df_alert["extra_fields"]["parent_candid"] = None (alert has no parent candid en extra field)
    if 'fp_hists' in extra_fields_columns: # extra condition, fp_hists is added in v4 schemas in 2023, so null_condition fails any date earlier
        forced_photometries_null_condition = extrafields_df_alert.select('fp_hists').item() # no fp_hists condition
        if forced_photometries_null_condition != None:
            forced_photometries_list = extrafields_df_alert.select('fp_hists')
            fp_hists = forced_photometries_list.explode('fp_hists').unnest('fp_hists')
        
            for row in range(len(fp_hists)):
                if ZTFForcedPhotometryParser.can_parse(fp_hists[row]):
                    parsed_forced_photometries = ZTFForcedPhotometryParser.parse_message(fp_hists[row], df_alert)
                    forced_photometries = polars.concat([forced_photometries, parsed_forced_photometries])
    
    
    #convert all detections/photometries to list of dicts
    
    dict_list_detections = detections.to_dicts()
    dict_list_non_detections = non_detections.to_dicts()
    dict_list_forced_photometries = forced_photometries.to_dicts()
    
    #create a dataframe of a single cell for each dict_list
    
    detection_list = polars.DataFrame({"detections" : [dict_list_detections]})
    non_detection_list = polars.DataFrame({"non_detections" : [dict_list_non_detections]})
    forced_photometries_list = polars.DataFrame({"forced_photometries" : [dict_list_forced_photometries]})
    
    #concat it to the dataframe of the alert
    
    df_alert = polars.concat([df_alert, detection_list], how = 'horizontal')
    df_alert = polars.concat([df_alert, non_detection_list], how = 'horizontal')
    df_alert = polars.concat([df_alert, forced_photometries_list], how = 'horizontal')
    
    return df_alert

# Receives a dataframe corresponding to avros parsed in sorting hat, and cycles through each alert, returning
# the updated dataframe with the detections, non-detections and forced photometries parsed from the extra fields
def extract_detections_and_non_detections_dataframe(df: polars.DataFrame):
    dataframe_updated = polars.DataFrame()
    for row in range(len(df)):
        print('processing row number: ', row)
        df_row = df[row] 
        alert_updated = extract_detections_and_non_detections(df_row)
        dataframe_updated = polars.concat([dataframe_updated, alert_updated], how = "diagonal_relaxed")
    return dataframe_updated
