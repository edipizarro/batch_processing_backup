import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cos, radians, when, abs, struct, explode

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, IntegerType, ShortType
spark = SparkSession.builder.config("spark.driver.host", "localhost").config("spark.driver.memory", "8g").config("spark.executor.memory", "4g").appName("SparkExample").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


conf = pyspark.SparkConf()
from pyspark.sql.functions import lit

import time
start_time = time.time()

import sorting_hat.sorting_hat_spark as sorting_hat
import prv_candidates.prv_candidates_pyspark as prv_candidates
import create_dataframes.create_dets_nds_phots_dataframes as create_dets_nds_phots_dataframes
import lightcurve_batch.light_curve_step_pyspark as lightcurve
import correction_batch.correction_step_pyspark as correction

df = sorting_hat.run_sorting_hat_step('parquets_avro')
print('Completed sorting hat step...')
print(f"Sorting hat execution time: {time.time() - start_time}")


df_prv_candidates = prv_candidates.extract_detections_and_non_detections_dataframe_reparsed(df)
print(f"Prv_candidates execution time: {time.time() - start_time}")
print('Completed prv candidates step...')



non_detections_frame = create_dets_nds_phots_dataframes.create_non_detections_frame(df_prv_candidates)
print('Completed non detections frame...')
detections_frame = create_dets_nds_phots_dataframes.create_detections_frame(df_prv_candidates)
print('Completed detections frame...')
forced_photometries_frame = create_dets_nds_phots_dataframes.create_forced_photometries_frame(df_prv_candidates)
print('Completed forced photometries frame...')
print(f"3 Frames separation execution time: {time.time() - start_time}")

print('Completed separation in 3 dataframes step...')

dataframe_lightcurve = lightcurve.execute_light_curve(detections_frame, non_detections_frame, forced_photometries_frame)
dataframe_lightcurve = dataframe_lightcurve.repartition(3000)
print('Completed light curve step...')
print(f"Light curve execution time: {time.time() - start_time}")

correction_dataframe = correction.produce_correction(dataframe_lightcurve)
print(correction_dataframe.count())
print('Completed correction dataframe...')
print(f"Correction step execution time: {time.time() - start_time}")

input("Press enter to terminate")
exit()
