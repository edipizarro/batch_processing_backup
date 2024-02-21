import click
import time
import logging


LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


@click.command()
@click.argument("input_objects", type=str)
@click.argument("input_detections", type=str)
@click.argument("input_non_detections", type=str)
@click.argument("output_objects", type=str)
@click.argument("output_detections", type=str)
@click.argument("output_non_detections", type=str)
@click.option("--n-partitions", "-n", default=300, help="Number of output parquet files")
@click.option("--log", "loglevel", default="INFO", help="log level to use", type=click.Choice(LOG_LEVELS))
def partition_data(
        input_objects,
        input_detections,
        input_non_detections,
        output_objects,
        output_detections,
        output_non_detections,
        n_partitions,
        loglevel,
):
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(filename="fill_missing_fields.log", level=numeric_level)

    start = time.time()

    # CONFIG
    conf = SparkConf()
    conf.set("spark.sql.shuffle.partitions", n_partitions)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # READ DATA
    # 1) Objects
    df_objects = spark.read.option(header=True).csv(input_objects)
    df_objects.repartition(n_partitions, "oid").write.format("parquet").bucketBy(n_partitions, "oid").option(
        "path", output_objects).saveAsTable("objects")

    # 2) Detections
    df_detections = spark.read.option(header=True).csv(input_detections)
    df_detections.repartition(n_partitions, "oid").write.format("parquet").bucketBy(n_partitions, "oid").option(
        "path", output_detections).saveAsTable("detections")

    # 3) Non detections
    df_non_detections = spark.read.option(header=True).csv(input_non_detections)
    df_non_detections.repartition(n_partitions, "oid").write.format("parquet").bucketBy(n_partitions, "oid").option(
        "path", output_non_detections).saveAsTable("non_detections")

    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))


if __name__ == "__main__":
    partition_data()
