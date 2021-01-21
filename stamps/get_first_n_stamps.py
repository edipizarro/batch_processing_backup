import time
import click
import logging
import math
import os

@click.command()
@click.argument("input_path", type=str)
@click.argument("output_path", type=str)
@click.option("--jd", "-j", default=58000.0, help="Filter objects by julian dates")
@click.option("--nstamps", "-n", default=1, help="Number of first n detections")
@click.option("--batch-size", "-b", default=20000, help="Number of first n detections")
@click.option("--partitions", "-p", help="Number of first n detections", type=int)
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def get_stamps(input_path, output_path, jd, nstamps, batch_size, partitions, loglevel):
    """
    Get first n-stamps given a list of oids

    INPUT_PATH is the name of the directory with alert avros. INPUT_PATH can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/object

    OUTPUT_PATH is the name of the output directory for detections parquet files.
    OUTPUT_PATH can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/detections

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
    sc = spark.sparkContext

    # read from bucket
    all_data = spark.read.format("avro").load(input_path)
    candids = all_data.select("objectId", "candid", "candidate.jd").dropDuplicates((['objectId', 'candid']))

    w = Window.partitionBy("objectId").orderBy("jd")
    candids = candids.withColumn("rownum", row_number().over(w)).where(col("rownum") <= nstamps).drop("rownum").filter(
        col("jd") >= jd)

    candids.write.save(os.path.join(output_path, "candids"))
    candids = [x.candid for x in candids.select('candid').collect()]
    candids = sc.broadcast(candids)

    selection = all_data.filter(col("candid").isin(candids.value))

    # select fields
    result = selection.select(
        "objectId",
        "candidate.*",
        col("cutoutDifference.stampData").alias("cutoutDifference"),
        col("cutoutScience.stampData").alias("cutoutScience"),
        col("cutoutTemplate.stampData").alias("cutoutTemplate")) \
        .withColumnRenamed("objectId", "oid")


    # select first n detections
    if partitions:
        number_partitions = partitions
    else:
        number_partitions = math.ceil(result.count() / batch_size)
    result.coalesce(number_partitions).write.save(os.path.join(output_path, "stamps"))
    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))
    return


if __name__ == "__main__":
    get_stamps()
