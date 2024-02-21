import sys
import time
import click
import logging


@click.command()
@click.argument("input_path", type=str)
@click.argument("output_path", type=str)
@click.argument("catalog_path", type=str)
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def xmatch(input_path, output_path, catalog_path, loglevel):
    """
    Make cross-match with catalog_path

    INPUT_PATH is the name of the directory with objects in parquet format. INPUT_PATH can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/object

    OUTPUT_PATH is the name of the output directory for xmatch parquet files.
    OUTPUT_PATH can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/xmatch

    CATALOG_PATH is the name of the directory of the external catalog

    """
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from astroide_wrapper import AstroideAPI

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(filename="fill_missing_fields.log", level=numeric_level)

    start = time.time()

    # initializing
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    api = AstroideAPI()
    # read external catalog
    df_ext_catalog = spark.read.load(catalog_path)
    df_input = spark.read.load(input_path)
    radius = 1.0 / 3600.0  # // 1 arcosegundo = 1 grado / 3600
    # reduce de input
    df_input = (
        df_input.withColumnRenamed("meanra", "ra")
        .withColumnRenamed("meandec", "dec")
        .select("objectId", "ra", "dec")
    )
    # make healpix index
    df_healpix = api.create_healpix_index(df_input, 12, "ra", "dec")
    # do the cross-match
    result = api.xmatch(df_ext_catalog, df_healpix, 12, radius, False)
    result.write.mode("overwrite").save(output_path)
    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))
    return


if __name__ == "__main__":
    xmatch()
