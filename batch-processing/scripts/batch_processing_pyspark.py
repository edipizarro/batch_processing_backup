import time

import pyspark
from pyspark.sql import SparkSession

from batch_processing.pipeline import (
    correction,
    create_dataframes,
    lightcurve,
    magstats,
    prv_candidates,
    sorting_hat,
    xmatch,
)
from batch_processing.pipeline.spark_init.pyspark_configs import *

start_time = time.time()

# Variables for Spark Session
compression = "snappy"
# JARS = (
#    "batch_processing/pipeline/spark_init/jars/healpix-1.0.jar, batch_processing/pipeline/spark_init/jars/minimal_astroide-2.0.0.jar"
# )
JARS = "libs/jars/healpix-1.0.jar, libs/jars/minimal_astroide-2.0.0.jar"

# Start SPARK Session
spark = (
    SparkSession.builder.config("spark.memory.fraction", "0.85")
    .config("spark.jars", JARS)
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.driver.memory", "9g")
    .config("spark.executor.memory", "6g")
    .master("local[5]")
    .appName("BatchingTesting")
    .getOrCreate()
)
spark.conf.set("spark.default.parallelism", "15")
spark.conf.set("spark.sql.shuffle.partitions", "15")

conf = pyspark.SparkConf()
sc = spark.sparkContext

## First, we run the sorting hat in all of the avro parquets in the folder of avro parquets
df_sorting_hat = sorting_hat.run_sorting_hat_step(
    spark, "/home/kay/Escritorio/github-batch/batch_processing/data/60292"
)
print("Completed sorting hat step...")
print(f"Sorting hat execution time: {time.time() - start_time}")

df_prv_candidates = (
    prv_candidates.extract_detections_and_non_detections_dataframe_reparsed(
        df_sorting_hat
    )
)
del df_sorting_hat
print(f"Prv_candidates execution time: {time.time() - start_time}")
print("Completed prv candidates step...")


non_detections_frame = create_dataframes.create_non_detections_frame(df_prv_candidates)
non_detections_frame.write.parquet(
    "output_batch/non_detections/", compression=compression, mode="overwrite"
)
non_detections_frame = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .schema(non_detections_frame.schema)
    .load("output_batch/non_detections")
)
print("Completed non detections frame...")

detections_frame = create_dataframes.create_detections_frame(df_prv_candidates)
detections_frame.write.parquet(
    "output_batch/detections/", compression=compression, mode="overwrite"
)
detections_frame = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .schema(detections_frame.schema)
    .load("output_batch/detections")
)
print("Completed detections frame...")

forced_photometries_frame = create_dataframes.create_forced_photometries_frame(
    df_prv_candidates
)
forced_photometries_frame.write.parquet(
    "output_batch/forced_photometries/", compression=compression, mode="overwrite"
)
forced_photometries_frame = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .schema(forced_photometries_frame.schema)
    .load("output_batch/forced_photometries")
)

forced_photometries_frame = forced_photometries_frame.distinct()
non_detections_frame = non_detections_frame.distinct()


del df_prv_candidates
print("Completed forced photometries frame...")
print(f"3 Frames separation execution time: {time.time() - start_time}")
print(
    "Completed separation in 3 dataframes step, including writing results to parquet..."
)

dataframe_lightcurve = lightcurve.execute_light_curve(
    detections_frame, non_detections_frame, forced_photometries_frame
)


del detections_frame, non_detections_frame, forced_photometries_frame
print("Completed light curve step including write time...")
print(f"Light curve execution time: {time.time() - start_time}")

correction_output = correction.produce_correction(dataframe_lightcurve)
correction_dataframe = correction_output[0]
forced_photometries_corrected = correction_output[1]
detections_corrected = correction_output[2]

del dataframe_lightcurve

correction_dataframe.write.parquet(
    "output_batch/corrections/", compression=compression, mode="overwrite"
)
forced_photometries_corrected.write.parquet(
    "output_batch/forced_photometries_corrected/",
    compression=compression,
    mode="overwrite",
)
detections_corrected.write.parquet(
    "output_batch/detections_corrected/", compression=compression, mode="overwrite"
)
del correction_output, forced_photometries_corrected, detections_corrected


correction_dataframe = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load("output_batch/corrections")
)

print("Completed correction dataframe...")
print(f"Correction step execution time: {time.time() - start_time}")

object_stats, magstats_stats = magstats.execute_magstats_step(correction_dataframe)
object_stats.write.parquet(
    "output_batch/objectstats/objectstats/", compression=compression, mode="overwrite"
)
magstats_stats.write.parquet(
    "output_batch/magstats/", compression=compression, mode="overwrite"
)
del correction_dataframe, object_stats, magstats_stats
print("Total time including magstats write: " + str(time.time() - start_time))


correction_dataframe_xmatch_data = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load("output_batch/corrections")
    .select(
        "oid",
        "meanra",
        "meandec",
        "detections.aid",
        "detections.ra",
        "detections.dec",
        "detections.forced",
        "detections.extra_fields.sgscore1",
        "detections.extra_fields.distpsnr1",
        "detections.extra_fields.sgmag1",
        "detections.extra_fields.srmag1",
    )
)


xmatch_step_output = xmatch.execute_batchsizes(spark, correction_dataframe_xmatch_data)
xmatch_step = xmatch_step_output[0]
unprocessed = xmatch_step_output[1]

if unprocessed != None:
    unprocessed.write.parquet(
        "output_batch/unprocessed/", compression=compression, mode="overwrite"
    )

xmatch_step_result = xmatch_step.repartition("oid")

xmatch_step_result.write.parquet(
    "output_batch/xmatch/", compression=compression, mode="overwrite"
)

print("Total time including xmatch write: " + str(time.time() - start_time))
input("Press enter to terminate")
spark.stop()
exit()
