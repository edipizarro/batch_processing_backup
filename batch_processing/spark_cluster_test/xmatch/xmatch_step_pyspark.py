from spark_init.pyspark_configs import *
from astropy.table import Table
import astropy.io.votable as votable
import requests
import pandas as pd
import io


CATALOG_MAP = {
    "allwise":"vizier:II/328/allwise"
}

# It's necessary to adjust this value as necessary
_BATCH_SIZE = 10000 # Number of simultaneous requests to CDS Xmatch service

_INSTANCES_NUMBER = 5

_NSIDE_RES = 9

_RETRIES = 3
_RETRY_INTERVAL = 1

XMATCH_CONFIG = {
    "CATALOG": {
        "name": "allwise",
        "columns": [
            "AllWISE",
            "RAJ2000",
            "DEJ2000",
            "W1mag",
            "W2mag",
            "W3mag",
            "W4mag",
            "e_W1mag",
            "e_W2mag",
            "e_W3mag",
            "e_W4mag",
            "Jmag",
            "e_Jmag",
            "Hmag",
            "e_Hmag",
            "Kmag",
            "e_Kmag",
            "eeMaj",
            "eeMin",
            "eePA",
            "ID",
            "ccf",
            "ex",
            "var",
            "qph",
            "pmRA", 
            "e_pmRA",
            "pmDE",
            "e_pmDE",
            "d2M"
        ],
    }
}

def separate_dataframe(dataframe):
    detections = dataframe.select("detections")
    exploded_dets = detections.select(explode("detections").alias("exploded_data"))
    detections_df = exploded_dets.select("exploded_data.*")
    non_detections = dataframe.select("non_detections")
    exploded_ndets = non_detections.select(explode("non_detections").alias("exploded_data"))
    non_detections_df = exploded_ndets.select("exploded_data.*")
    return detections_df, non_detections_df

class XmatchClient:
    @staticmethod
    def execute( 
        catalog,
        catalog_type: str,
        ext_catalog: str,
        ext_columns: list,
        selection: str,
        result_type: str,
        distmaxarcsec: int = 1,):

        try:
            # catalog alias
            if ext_catalog in CATALOG_MAP:
                ext_catalog = CATALOG_MAP[ext_catalog]

            # Encode input
            if catalog_type == "pandas":
                string_io = io.StringIO()
                new_columns = {}
                for c in catalog.columns:
                    new_columns[c] = "%s_in" % c
                catalog.rename(columns=new_columns, inplace=True)

                catalog.to_csv(string_io)
                catalog_content = string_io.getvalue()

            elif catalog_type == "astropy":
                columns = list(catalog.columns)
                for c in columns:
                    catalog.rename_column(c, "%s_in" % c)

                bytes_io = io.BytesIO()
                catalog = votable.from_table(catalog)
                votable.writeto(catalog, bytes_io)
                catalog_content = bytes_io.getvalue()

            else:
                raise Exception("Unknown input type %s" % catalog_type)

            # columns
            ext_columns_str = None
            if ext_columns is not None:
                ext_columns_str = ",".join(ext_columns)

            # response format
            if result_type == "pandas":
                response_format = "csv"
            elif result_type == "astropy":
                response_format = "votable"
            else:
                raise Exception("Unknown output type %s" % result_type)

            # params
            params = {
                "request": "xmatch",
                "distMaxArcsec": distmaxarcsec,
                "selection": selection,
                "RESPONSEFORMAT": response_format,
                "cat2": ext_catalog,
                "colRA1": "ra_in",
                "colDec1": "dec_in",
            }
            if ext_columns_str is not None:
                params["cols2"] = ext_columns_str

            # Send the request
            r = requests.post(
                "http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync",
                data=params,
                files={"cat1": catalog_content},
            )
            if not r.ok:
                raise Exception(f"CDS Request returned status {r.status_code}")

            # Decode output
            response_bytes = io.BytesIO(r.content)
            result = None
            if result_type == "pandas":
                result = pd.read_csv(response_bytes)

            elif result_type == "astropy":
                result = Table.read(response_bytes)

        except Exception as exception:
            print("Request to CDS xmatch failed: %s \n" % exception)
            raise exception

        return result



def xmatch_parameters():
     return {
        "catalog_alias": XMATCH_CONFIG['CATALOG']["name"],            
        "columns": XMATCH_CONFIG['CATALOG']["columns"],    
        "radius": 1,
        "selection": "best",
        "input_type": "pandas",
        "output_type": "pandas",
        }

def request_xmatch(input_catalog, retries_count, xmatch_client, parameters, retry_interval):
    if retries_count > 0:
            try:
                result = xmatch_client.execute(
                    input_catalog,
                    parameters["input_type"],
                    parameters["catalog_alias"],
                    parameters["columns"],
                    parameters["selection"],
                    parameters["output_type"],
                    parameters["radius"],
                )
                return result

            except Exception as e:
                print(
                    f"CDS xmatch client returned with error {e}"
                )
                time.sleep(retry_interval)
                print("Retrying request")
                return request_xmatch(input_catalog, retries_count - 1, xmatch_client, parameters, retry_interval)

    if retries_count == 0:
            print(
                f"Retrieving xmatch from the client unsuccessful after {retries_count} retries. Shutting down."
            )
            raise Exception(
                f"Could not retrieve xmatch from CDS after {retries_count} retries."
            )
    return

def pandas_dataframe_chunks(dataframe, batch_size):
    list_dataframes = [dataframe[i:i+batch_size] for i in range(0,len(dataframe),batch_size)]
    return list_dataframes

def execute_xmatch_requests(dataframe_pandas):
        """Perform xmatch against CDS."""
        print(f"Processing {len(dataframe_pandas)} light curves")
        xmatch_client = XmatchClient()
        params = xmatch_parameters()
        print("Getting xmatches...")
        xmatches = request_xmatch(dataframe_pandas, _RETRIES, xmatch_client, params, _RETRY_INTERVAL)
        return xmatches

def execute_batchsizes(dataframe_spark):
    print(f"Processing using a batch size of {_BATCH_SIZE} oids per request to CDS")
    dataframe_crossmatch = dataframe_spark.selectExpr("oid", "meanra as ra", "meandec as dec")
    api = AstroideAPI()
    reference_healpix = api.create_healpix_index(dataframe_crossmatch, _NSIDE_RES, "ra", "dec")
    reference_healpix = reference_healpix.sort('ipix')
    dataframe_crossmatch = reference_healpix.toPandas()
    list_dataframes = pandas_dataframe_chunks(dataframe_crossmatch, _BATCH_SIZE)
    xmatch_batch_results = pd.DataFrame()
    for chunk in range(len(list_dataframes)):
        print(f"Executing batch number {chunk} of {len(list_dataframes)}")
        print(f"Batch {chunk} has a size of {len(list_dataframes[chunk])} oids to process")
        xmatches = execute_xmatch_requests(list_dataframes[chunk].copy()) 
        xmatch_batch_results = pd.concat([xmatch_batch_results, xmatches])
    xmatches_spark = spark.createDataFrame(xmatch_batch_results).withColumnRenamed("oid_in", "oid").drop("ra_in").drop("dec_in").drop("col1")
    dataframe_spark = dataframe_spark.repartition('oid')
    xmatches_spark = xmatches_spark.repartition('oid')
    xmatch_step = dataframe_spark.join(xmatches_spark, on="oid", how="left")

    # Add the W1-W2, W2-W3 columns
    xmatch_step = xmatch_step.withColumn("W1-W2", xmatch_step.W1mag - xmatch_step.W2mag)\
                                               .withColumn("W2-W3", xmatch_step.W2mag - xmatch_step.W3mag)
    
    # Expand all the detections rows
    xmatch_step = xmatch_step.withColumn("expanded_columns", F.arrays_zip("aid", "ra", "dec", "forced", "sgscore1", "distpsnr1", "sgmag1", "srmag1"))\
       .withColumn("expanded_columns", F.explode("expanded_columns"))\
       .select('oid', 'meanra', 'meandec', 'angDist', 'AllWISE', 'RAJ2000', 'DEJ2000', 'eeMaj', 
               'eeMin', 'eePA', 'W1mag', 'W2mag', 'W3mag', 'W4mag', 'Jmag', 'Hmag', 'Kmag', 
               'e_W1mag', 'e_W2mag', 'e_W3mag', 'e_W4mag', 'e_Jmag', 'e_Hmag', 'e_Kmag', 'ID', 
               'ccf', 'ex', 'var', 'qph', 'pmRA', 'e_pmRA', 'pmDE', 'e_pmDE', 'd2M', 'W1-W2', 'W2-W3',
                F.col("expanded_columns.aid").alias("aid"),
                F.col("expanded_columns.ra").alias("ra_oid"),
                F.col("expanded_columns.dec").alias("dec_oid"), 
                F.col("expanded_columns.forced").alias("forced"), 
                F.col("expanded_columns.sgscore1").alias("sgscore1"), 
                F.col("expanded_columns.distpsnr1").alias("distpsnr1"), 
                F.col("expanded_columns.sgmag1").alias("sgmag1"), 
                F.col("expanded_columns.srmag1").alias("srmag1"))
    
    window_spec = Window.partitionBy("oid")

    # Replace the null values of the columns from alert, by using for each oid the data of one alert 
    xmatch_step = xmatch_step.withColumn("sgscore1", first("sgscore1", ignorenulls=True).over(window_spec)) \
                   .withColumn("distpsnr1", first("distpsnr1", ignorenulls=True).over(window_spec)) \
                   .withColumn("sgmag1", first("sgmag1", ignorenulls=True).over(window_spec)) \
                   .withColumn("srmag1", first("srmag1", ignorenulls=True).over(window_spec))
    
    return xmatch_step
