import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, explode, array, coalesce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, IntegerType, ShortType
spark = SparkSession.builder.config("spark.driver.host", "localhost").appName("SparkExample").getOrCreate()
conf = pyspark.SparkConf()
from pyspark.sql.functions import lit

spark_context = SparkSession.builder.config(conf=conf).getOrCreate()


# Fields to replace during the extra_field update
repeated_extra_fields = ['magap', 'pdiffimfilename', 'rcid', 'seeratio', 'rbversion', 'jdendref', 'rfid', 'ssnrms', 'clrrms', 'simag3', 'distnr', 'diffmaglim', 'szmag2', 'ndethist', 'zpclrcov', 'nmtchps', 'maggaiabright', 'nmatches', 'sky', 'scorr', 'jdstarthist', 'candid', 'bimagerat', 'clrcoeff', 'neargaiabright', 'nbad', 'objectidps1', 'elong', 'programid', 'magzpsciunc', 'szmag3', 'jdendhist', 'simag2', 'chinr', 'sgmag1', 'szmag1', 'distpsnr2', 'jdstartref', 'magzpsci', 'tooflag', 'sgmag3', 'exptime', 'sumrat', 'drbversion', 'sgmag2', 'magfromlim', 'maggaia', 'nframesref', 'programpi', 'mindtoedge', 'objectidps2', 'sigmagnr', 'srmag2', 'ypos', 'tblid', 'aimage', 'field', 'dsdiff', 'ssmagnr', 'xpos', 'ranr', 'simag1', 'nneg', 'aimagerat', 'unique_id', 'sgscore2', 'srmag3', 'clrcounc', 'oid', 'srmag1', 'sharpnr', 'sigmagap', 'classtar', 'nid', 'ssdistnr', 'ncovhist', 'fwhm', 'decnr', 'magdiff', 'sgscore3', 'bimage', 'distpsnr3', 'chipsf', 'zpmed', 'ssnamenr', 'clrmed', 'dsnrms', 'distpsnr1', 'drb', 'sigmagapbig', 'neargaia', 'sgscore1', 'magnr', 'objectidps3', 'magapbig', 'rb', 'magzpscirms']
# Fields to coalesce during join alert and detections
fields_alert_detections = ['tid', 'sid', 'pid', 'mjd', 'fid', 'ra', 'dec', 'mag', 'e_mag', 'isdiffpos', 'e_ra', 'e_dec', 'forced', 'has_stamp', 'parent_candid', 'unparsed_jd', 'unparsed_fid', 'unparsed_isdiffpos', 'aid']






# Create the dataframe of the unique non_detections
def create_non_detections_frame(df):
    non_detections_frame = df.select('non_detections')
    # Explode the non_detections to not have them as a list of dicts
    exploded_non_detections = non_detections_frame.select(explode(non_detections_frame.non_detections).alias("exploded_data"))
    # We select the exploded data, creating a new dataframe with columns accesible
    unnested_non_detections = exploded_non_detections.select("exploded_data.*")
    # We use distinct to keep only the first ocurrence of a non-detection
    unique_non_detections = unnested_non_detections.drop_duplicates(['oid', 'mjd'])

    return unique_non_detections

def create_forced_photometries_frame(df):
    df = df.sort('mjd', ascending = False)
    fp_frame = df.select('forced_photometries')
    exploded_fp = fp_frame.select(explode(fp_frame.forced_photometries).alias("exploded_data"))
    unnested_fp  = exploded_fp.select("exploded_data.*")
    unique_fp  = unnested_fp.drop_duplicates(['oid', 'candid'])
    return unique_fp 


# Now we want to get the detections dataframe. 
def create_detections_frame(df):
    # We'll first order the dataframe by MJD. This is to retain the newest alerts
    df = df.sort('mjd', ascending = False)
    # We'll create an alert dataframe. It contains only the alerts (without dets/nondets/fp_hists)
    alert_df = df.drop('detections', 'non_detections', 'forced_photometries')
    alert_df = alert_df.drop_duplicates(['oid', 'candid'])
    # Then we need to expand the alerts' extra_fields and add the oid and candids to join them back later on
    alert_extra_fields = alert_df.select(
    struct(*[col('extra_fields.' + field) for field in alert_df.select('extra_fields.*').columns] +
           [col('oid').alias('oid'), col('candid').alias('candid')]).alias('extra_fields')
    )
    
    alert_extra_fields = alert_extra_fields.select('extra_fields').select(col("extra_fields.*"))
    # We drop the extra_fields column from the alert dataframe, to update it in the end
    alert_df = alert_df.drop('extra_fields')
    

    #! Then we work on getting the detections frame and its extra_fields
    # Now we get the detections dataframe from the original dataframe. We'll extract the extra_fields from the detections dataframe and use it later to update
    # First we get the detections dataframe
    detections_frame = df.select('detections')
    # Explode the detections to not have them as a list of dicts
    exploded_detections = detections_frame.select(explode(detections_frame.detections).alias("exploded_data"))
    # We select the exploded data, creating a new dataframe with columns accesible
    unnested_detections = exploded_detections.select("exploded_data.*")
    # And drop duplicates by oid and candid
    unique_detections = unnested_detections.drop_duplicates(['oid', 'candid'])
    # Now we extract the extra_fields from the detections dataframe
    unique_detections_extra_fields = unique_detections.select(
    struct(*[col('extra_fields.' + field) for field in unique_detections.select('extra_fields.*').columns] +
           [col('oid').alias('oid'), col('candid').alias('candid')]).alias('extra_fields')
    )
    unique_detections_extra_fields = unique_detections_extra_fields.select('extra_fields').select(col("extra_fields.*"))
    # We drop the extra_fields column from the  unique detections dataframe, to update it in the end
    unique_detections = unique_detections.drop('extra_fields')

    # Now we'll join the two extra fields dataframes
    # Add a suffix to distinguish between both set of extra fields during next steps. We will drop the unnecesary one afterwards
    unique_detections_extra_fields = unique_detections_extra_fields.selectExpr([f"{col} as {col}_detection" for col in unique_detections_extra_fields.columns])
    unique_detections_extra_fields = unique_detections_extra_fields \
                                .selectExpr("oid_detection AS oid", "candid_detection AS candid")

    # Add a suffix to distinguish between alert and detections
    unique_detections = unique_detections.selectExpr([f"{col} as {col}_detection" for col in unique_detections.columns])
    unique_detections = unique_detections.selectExpr("oid_detection AS oid", "candid_detection AS candid")

    # We join right both the dataframes of alerts, and then the dataframes of extra fields
    join_extra_fields = alert_extra_fields.join(unique_detections_extra_fields, on = ['oid', 'candid'], how = 'full')

    # Search in the repeated columns for the columns that must be used in alerts/detections' extra fields
    # We'll use coalesce in the repeated columns to update the dataframe
    for column_name in repeated_extra_fields:
    # Check if a column with 'detection' suffix exists
        if column_name + "_detection" in join_extra_fields.columns:
        # Replace values in the column with no suffix with the values from the column with 'detection' suffix
            join_extra_fields = join_extra_fields \
                .selectExpr(
                    f"coalesce({column_name + '_detection'}, {column_name}) AS {column_name}"
                ) \
                .drop(column_name + "_detection")
            
    # Join extra fields is complete. Now we must join the alerts and the detections correctly to proceed       
    join_alertas_detections = alert_df.join(unique_detections, on = ['oid', 'candid'], how = 'full')
    

    for column_name in fields_alert_detections:
        # Check if a column with 'detection' suffix exists
        if column_name + "_detection" in join_alertas_detections.columns:
            # Replace values in the column with no suffix with the values from the column with 'detection' suffix
            join_alertas_detections = join_alertas_detections \
                .selectExpr(
                    f"coalesce({column_name + '_detection'}, {column_name}) AS {column_name}"
                ) \
                .drop(column_name + "_detection")
    # Nesting the extra fields back into a single column    
    columns_to_nest = [col for col in join_extra_fields.columns if col not in ['oid', 'candid']]
    nested_col = struct(*columns_to_nest)
    join_extra_fields = join_extra_fields.select(*join_extra_fields.columns, nested_col.alias("extra_fields"))
    extra_fields_df = join_extra_fields.select('oid', 'candid', 'extra_fields')
    
    # Joining the alerts/detections back with their respective extra_fields
    detections_dataframe = join_alertas_detections.join(extra_fields_df, on = ['oid', 'candid'], how = 'left')
    detections_dataframe = detections_dataframe.select(sorted(detections_dataframe.columns))

    return detections_dataframe



