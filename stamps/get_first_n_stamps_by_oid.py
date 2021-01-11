import time
import click
import logging

import pandas as pd

@click.command()
@click.argument("input_path", type=str)
@click.argument("output_path", type=str)
@click.argument("oids_file_path", type=str)
@click.option("--nstamps", "-n", default=2, help="Number of first n detections")
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def get_stamps_by_oid(input_path, output_path, oids_file_path,  nstamps, loglevel):
    """
    Get first n-stamps given a list of oids

    INPUT_PATH is the name of the directory with alert avros. INPUT_PATH can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/object

    OUTPUT_PATH is the name of the output directory for detections parquet files.
    OUTPUT_PATH can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/detections

    OIDS_FILE_PATH is the name of the path to csv-file with oids. I

    """
    from pyspark import SparkConf
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.functions import col, row_number

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(filename="fill_missing_fields.log", level=numeric_level)

    start = time.time()

    # CONFIG
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    oids = pd.read_csv(oids_file_path)
    oids = list(oids["oid"].values)

    # read from bucket
    ztf = spark.read.format("avro").load(input_path)
    ztf = ztf.filter(col("objectId").isin(list(oids)))

    # select fields
    selection = ztf.select(
        "objectId",
        "candidate.*",
        col("cutoutDifference.stampData").alias("cutoutDifference"),
        col("cutoutScience.stampData").alias("cutoutScience"),
        col("cutoutTemplate.stampData").alias("cutoutTemplate")) \
        .withColumnRenamed("objectId", "oid")

    selection = selection.dropDuplicates((['oid', 'candid']))

    # select first n detections
    w = Window.partitionBy("oid").orderBy("jd")
    result = selection.withColumn("rownum", row_number().over(w)) \
        .where(col("rownum") <= nstamps) \
        .drop("rownum")

    result.write.save(output_path)
    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))
    return


if __name__ == "__main__":
    get_stamps_by_oid()

