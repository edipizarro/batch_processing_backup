import argparse
import time

from batch_processing.pipeline import correction
from batch_processing.pipeline import create_dataframes
from batch_processing.pipeline import magstats
from batch_processing.pipeline import sorting_hat
from batch_processing.pipeline import prv_candidates
from batch_processing.pipeline import lightcurve
from batch_processing.pipeline import xmatch
from batch_processing.pipeline.spark_init.pyspark_configs import *

parser = argparse.ArgumentParser()
parser.add_argument("--data_source")
parser.add_argument("--output_folder")
args = parser.parse_args()
data_source = args.data_source
data_output = args.output_folder

start_time = time.time()

## First, we run the sorting hat in all of the avro parquets in the folder of avro parquets

df_sorting_hat = sorting_hat.run_sorting_hat_step(data_source, spark)
print('Completed sorting hat step...')
print(f"Sorting hat execution time: {time.time() - start_time}")

df_prv_candidates = prv_candidates.extract_detections_and_non_detections_dataframe_reparsed(df_sorting_hat)
del df_sorting_hat
print(f"Prv_candidates execution time: {time.time() - start_time}")
print('Completed prv candidates step...')


non_detections_frame = create_dataframes.create_non_detections_frame(df_prv_candidates)
non_detections_output = data_output + "/non_detections"
non_detections_frame.write.parquet(non_detections_output, compression = 'snappy', mode='overwrite')
non_detections_frame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(non_detections_frame.schema).load(non_detections_output + "/**/*")
print('Completed non detections frame...')

detections_frame = create_dataframes.create_detections_frame(df_prv_candidates)
detections_frame.filter(col('oid')=='ZTF18abvtinv').show()
detections_output = data_output + "/detections"
detections_frame.write.parquet(detections_output, compression = 'snappy', mode='overwrite')
detections_frame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(detections_frame.schema).load(detections_output + "/**/*")
print('Completed detections frame...')

forced_photometries_frame = create_dataframes.create_forced_photometries_frame(df_prv_candidates)
forced_photometries_output = data_output + "/forced_photometries"
forced_photometries_frame.write.parquet(forced_photometries_output, compression = 'snappy', mode='overwrite')
forced_photometries_frame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(forced_photometries_frame.schema).load(forced_photometries_output + "/**/*")

forced_photometries_frame = forced_photometries_frame.distinct()
non_detections_frame = non_detections_frame.distinct()
print(f'Total number of detections: ', detections_frame.count())
print(f' Total number of forced photometries: ', forced_photometries_frame.count())
print(f' Total number of non detections: ', non_detections_frame.count())
print('Total number of detections summed: ', detections_frame.count() + forced_photometries_frame.count())


del df_prv_candidates
print('Completed forced photometries frame...')
print(f"3 Frames separation execution time: {time.time() - start_time}")
print('Completed separation in 3 dataframes step, including writing results to parquet...')

dataframe_lightcurve = lightcurve.execute_light_curve(detections_frame, non_detections_frame, forced_photometries_frame)


del detections_frame, non_detections_frame, forced_photometries_frame
print('Completed light curve step including write time...') 
print(f"Light curve execution time: {time.time() - start_time}")

correction_dataframe = correction.produce_correction(dataframe_lightcurve)
del dataframe_lightcurve

correction_output = data_output + "/correction"
correction_dataframe.write.parquet(correction_output, compression = 'snappy', mode='overwrite')
correction_dataframe = spark.read.format("parquet").option("recursiveFileLookup", "true").load(correction_output + "/**/*")

print('Completed correction dataframe...')
print(f"Correction step execution time: {time.time() - start_time}")

object_stats, magstats_stats = magstats.execute_magstats_step(correction_dataframe)
objectstats_output = data_output + "/objectstats"
magstats_output = data_output + "/magstats"
object_stats.write.parquet(objectstats_output, compression = 'snappy', mode='overwrite')#
magstats_stats.write.parquet(magstats_output, compression = 'snappy', mode='overwrite')
print('Total time including magstats write: ' + str(time.time() - start_time))


correction_dataframe_xmatch_data = spark.read.format("parquet").option("recursiveFileLookup", "true").load(correction_output + "/**/*")\
                            .select('oid', 'meanra', 'meandec', 'detections.aid', 'detections.ra', 'detections.dec', 'detections.forced', 'detections.extra_fields.sgscore1',
                                   'detections.extra_fields.distpsnr1', 'detections.extra_fields.sgmag1', 'detections.extra_fields.srmag1')

xmatch_step_output = xmatch.execute_batchsizes(correction_dataframe_xmatch_data)
xmatch_step = xmatch_step_output[0]
unprocessed = xmatch_step_output[1]

print(f"Number of xmatch result rows: ", xmatch_step.count())

if unprocessed!=None:
    print(f"Wrote to disk: ", unprocessed.count(), " unprocessed light curves")
    unprocessed_output = data_output + "/unprocessed"
    unprocessed.write.parquet(unprocessed_output, compression = 'snappy', mode='overwrite')

xmatch_step_result = xmatch_step.repartition("oid")
xmatch_output = data_output + "/xmatch"
xmatch_step_result.write.parquet(xmatch_output, compression = 'snappy', mode='overwrite')

print('Total time including xmatch write: ' + str(time.time() - start_time))
spark.stop()
