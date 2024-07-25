from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from .candidate import candidate_schema
from .cutout import cutout_schema
from .fp_hist import fp_hist_schema
from .prv_candidate import prv_candidate_schema

avro_schema_alert = StructType([
    StructField("schemavsn", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("objectId", StringType(), True),
    StructField("candid", LongType(), True),
    StructField("candidate", candidate_schema, True),
    StructField("prv_candidates", ArrayType(prv_candidate_schema), True),
    StructField("fp_hists", ArrayType(fp_hist_schema), True),
    StructField("cutoutScience", cutout_schema, True),
    StructField("cutoutTemplate", cutout_schema, True),
    StructField("cutoutDifference", cutout_schema, True)
])