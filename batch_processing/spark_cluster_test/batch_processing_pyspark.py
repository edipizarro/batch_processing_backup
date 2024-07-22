from spark_init.pyspark_configs import *
import sorting_hat.sorting_hat_spark as sorting_hat
import prv_candidates.prv_candidates_pyspark as prv_candidates
import create_dataframes.create_dets_nds_phots_dataframes as create_dets_nds_phots_dataframes
import lightcurve_batch.light_curve_step_pyspark as lightcurve
import correction_batch.correction_step_pyspark as correction
import magstats.magstats_pyspark as magstats
import xmatch.xmatch_step_pyspark_refactor as xmatch
import time
start_time = time.time()

## First, we run the sorting hat in all of the avro parquets in the folder of avro parquets

df_sorting_hat = sorting_hat.run_sorting_hat_step('sample_avroparquets', spark)
df_sorting_hat = df_sorting_hat
print('Completed sorting hat step...')
print(f"Sorting hat execution time: {time.time() - start_time}")

df_prv_candidates = prv_candidates.extract_detections_and_non_detections_dataframe_reparsed(df_sorting_hat)
df_prv_candidates = df_prv_candidates
del df_sorting_hat
print(f"Prv_candidates execution time: {time.time() - start_time}")
print('Completed prv candidates step...')


non_detections_frame = create_dets_nds_phots_dataframes.create_non_detections_frame(df_prv_candidates)
non_detections_frame.write.parquet("output_batch/non_detections/non_detections_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')
non_detections_frame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(non_detections_frame.schema).load("output_batch/non_detections")
print('Completed non detections frame...')

detections_frame = create_dets_nds_phots_dataframes.create_detections_frame(df_prv_candidates)
detections_frame.write.parquet("output_batch/detections/detections_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')
detections_frame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(detections_frame.schema).load("output_batch/detections")
print('Completed detections frame...')

forced_photometries_frame = create_dets_nds_phots_dataframes.create_forced_photometries_frame(df_prv_candidates)
forced_photometries_frame.write.parquet("output_batch/forced_photometries/forced_photometries_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')
forced_photometries_frame = spark.read.format("parquet").option("recursiveFileLookup", "true").schema(forced_photometries_frame.schema).load("output_batch/forced_photometries")

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
dataframe_lightcurve = dataframe_lightcurve

del detections_frame, non_detections_frame, forced_photometries_frame
print('Completed light curve step including write time...') 
print(f"Light curve execution time: {time.time() - start_time}")

correction_dataframe = correction.produce_correction(dataframe_lightcurve)
correction_dataframe =correction_dataframe
#del dataframe_lightcurve

correction_dataframe.write.parquet("output_batch/corrections/corrections_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')
correction_dataframe = spark.read.format("parquet").option("recursiveFileLookup", "true").load("output_batch/corrections")
print('Completed correction dataframe...')
print(f"Correction step execution time: {time.time() - start_time}")

object_stats, magstats_stats = magstats.execute_magstats_step(correction_dataframe)
object_stats.write.parquet("output_batch/objectstats/objectstats/objstats_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')#
magstats_stats.write.parquet("output_batch/magstats/magstats_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')#
print('Total time including magstats write: ' + str(time.time() - start_time))


correction_dataframe_xmatch_data = spark.read.format("parquet").option("recursiveFileLookup", "true").load("output_batch/corrections")\
                            .select('oid', 'meanra', 'meandec', 'detections.aid', 'detections.ra', 'detections.dec', 'detections.forced', 'detections.extra_fields.sgscore1',
                                   'detections.extra_fields.distpsnr1', 'detections.extra_fields.sgmag1', 'detections.extra_fields.srmag1')

xmatch_step_output = xmatch.execute_batchsizes(correction_dataframe_xmatch_data)
xmatch_step = xmatch_step_output[0]
unprocessed = xmatch_step_output[1]

print(f"Number of xmatch result rows: ", xmatch_step.count())

if unprocessed!=None:
    print(f"Wrote to disk: ", unprocessed.count(), " unprocessed light curves")
    unprocessed.write.parquet("output_batch/unprocessed/unprocessed_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')

xmatch_step_result = xmatch_step.repartition("oid")
xmatch_step_result.write.parquet("output_batch/xmatch/xmatch_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')

print('Total time including xmatch write: ' + str(time.time() - start_time))
input("Press enter to terminate")
spark.stop()
exit()
