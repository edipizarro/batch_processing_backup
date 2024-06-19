import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cos, radians, when, abs, struct, explode
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, IntegerType, ShortType
spark = SparkSession.builder.config("spark.driver.host", "localhost").config("spark.driver.memory", "9g").config("spark.executor.memory", "1g").master("local[*]").appName("Sparktestspp").getOrCreate()

conf = pyspark.SparkConf()
sc = spark.sparkContext
# Set the checkpoint directory
sc.setCheckpointDir("checkpoint/dir")
from pyspark.sql.functions import lit
import time
start_time = time.time()

import sorting_hat.sorting_hat_spark as sorting_hat
import prv_candidates.prv_candidates_pyspark as prv_candidates
import create_dataframes.create_dets_nds_phots_dataframes as create_dets_nds_phots_dataframes
import lightcurve_batch.light_curve_step_pyspark as lightcurve
import correction_batch.correction_step_pyspark as correction
import magstats.magstats_pyspark as magstats

## First, we run the sorting hat in all of the avro parquets in the folder of avro parquets
## We must modify to allow the use of the dets/ndets/phots dataframe to not save unnecessary parquets
df = sorting_hat.run_sorting_hat_step('parquets_avro', spark)
df = df.localCheckpoint()
print('Completed sorting hat step...')
print(f"Sorting hat execution time: {time.time() - start_time}")


df_prv_candidates = prv_candidates.extract_detections_and_non_detections_dataframe_reparsed(df)
df_prv_candidates = df_prv_candidates.localCheckpoint()
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
print('Completed forced photometries frame...')
print(f"3 Frames separation execution time: {time.time() - start_time}")
print('Completed separation in 3 dataframes step, including writing results to parquet...')


dataframe_lightcurve = lightcurve.execute_light_curve(detections_frame, non_detections_frame, forced_photometries_frame)
dataframe_lightcurve = dataframe_lightcurve.localCheckpoint()
print('Completed light curve step including write time...') 
print(f"Light curve execution time: {time.time() - start_time}")

correction_dataframe = correction.produce_correction(dataframe_lightcurve)
correction_dataframe = correction_dataframe.localCheckpoint()
print('Completed correction dataframe...')
print(f"Correction step execution time: {time.time() - start_time}")


objectstats_frame = magstats.execute_objstats(correction_dataframe)
objectstats_frame.write.parquet("output_batch/objectstats/objectstats/objstats_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')

magstats_frame = magstats.execute_magstats(correction_dataframe)
magstats_frame.write.parquet("output_batch/magstats/magstats_parquet.snappy.parquet", compression = 'snappy', mode='overwrite')
print('Completed magstats step...')
print('Total time including magstats write: ' + str(time.time() - start_time))
input("Press enter to terminate")
spark.stop()
exit()
