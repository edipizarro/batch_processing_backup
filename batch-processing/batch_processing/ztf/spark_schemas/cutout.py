from pyspark.sql.types import StructType, StructField, StringType, BinaryType

cutout_schema = StructType([
    StructField("fileName", StringType(), True),
    StructField("stampData", BinaryType(), True)
])
