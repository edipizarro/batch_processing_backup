import click
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, coalesce, round
import time
import logging


@click.command()
@click.argument("source_avros", type=str)
@click.argument("output_detections", type=str)
@click.argument("output_non_detections", type=str)
@click.argument("schema_path", type=str)
@click.option("--npartitions", "-n", default=300, help="Number of output parquet files")
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def main(
    source_avros,
    output_detections,
    output_non_detections,
    schema_path,
    npartitions,
    loglevel,
):
    """
    Partititon detection and non detections into parquet files

    SOURCE_AVROS is the name of the directory with alert avros. SOURCE_AVROS can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/object

    OUTPUT_DETECTIONS is the name of the output directory for detections parquet files.
    OUTPUT_DETECTIONS can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/detections

    OUTPUT_NON_DETECTIONS is the name of the output directory for non detections parquet files.
    OUTPUT_NON_DETECTIONS can be a local directory or a URI. For example a S3 URI like
    s3a://ztf-historic-data/detections
    """
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(filename="fill_missing_fields.log", level=numeric_level)

    start = time.time()

    # CONFIG
    conf = SparkConf()
    conf.set("spark.sql.shuffle.partitions", npartitions)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    with open(schema_path, "r") as f:
        schema = f.read()

    # READ AVROS
    df = (
        spark.read.format("avro")
        .option("avroSchema", schema)
        .load(source_avros)
        .drop("cutoutScience")
        .drop("cutoutTemplate")
        .drop("cutoutDifference")
        .repartition(npartitions, "objectId")
        .cache()
    )

    ##PRV CANDIDATES
    prv_candid = (
        df.select(
            "objectId",
            explode("prv_candidates").alias("prv_candidates"),
            col("candid").alias("parent_candid"),
        )
        .select("objectId", "parent_candid", "prv_candidates.*")
        .cache()
    )

    # NON DETECTIONS
    non_det = (
        prv_candid.where(col("candid").isNull())
        .withColumn("d", round(col("jd"), 5))
        .dropDuplicates(["objectId", "d"])
        .drop("d")
    )

    # DETECTIONS
    det = df.select("objectId", "candidate.*").dropDuplicates(["objectId", "candid"])

    prv_det = prv_candid.where(col("candid").isNotNull()).dropDuplicates(
        ["objectId", "candid"]
    )

    det_col = set(det.columns)
    prv_col = set(prv_det.columns)
    diff_col = list(det_col - prv_col)
    prv_col.remove("parent_candid")
    prv_col.remove("objectId")
    prv_col.remove("candid")

    tt_det = (
        prv_det.alias("p")
        .join(det.alias("d"), ["objectId", "candid"], "outer")
        .select(
            "objectId",
            "candid",
            "parent_candid",
            *[coalesce(col("d." + c), col("p." + c)).alias(c) for c in prv_col],
            *diff_col
        )
    )

    tt_det = tt_det.fillna(0, "parent_candid")

    tt_det.repartition(npartitions, "objectId").write.format("parquet").bucketBy(
        npartitions, "objectId"
    ).option("path", output_detections).saveAsTable("detections")

    non_det.repartition(npartitions, "objectId").write.format("parquet").bucketBy(
        npartitions, "objectId"
    ).option("path", output_non_detections).saveAsTable("non_detections")

    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))


if __name__ == "__main__":
    main()
