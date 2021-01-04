from load_psql.loaders import (
    DetectionsCSVLoader,
    NonDetectionsCSVLoader,
    ObjectsCSVLoader,
    SSCSVLoader,
    MagstatsCSVLoader,
    PS1CSVLoader,
    ReferenceCSVLoader,
    GaiaCSVLoader,
    DataQualityCSVLoader,
)

from pyspark.sql import SparkSession, Window
from pyspark import SparkConf, SparkContext
import click
import os
import sys


def create_session():

    # logging.info("Creating spark session")
    conf = SparkConf()
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_tt_det(session, det_src, raw_det_src):
    df = session.read.load(det_src)
    dfc = session.read.load(raw_det_src)
    return df.alias("i").join(dfc.alias("c"), ["objectId", "candid"], "inner")


def validate_config(config):
    all_tables = [
        "detection",
        "non_detection",
        "object",
        "magstats",
        "ps1",
        "ss",
        "reference",
        "dataquality",
        "xmatch",
        "probability",
        "feature",
    ]
    csv_loader_options = ["n_partitions", "max_records_per_file", "mode"]
    if "db" not in config:
        return False, "missing db key in config"
    if "tables" not in config:
        return False, "missing tables key in config"
    if "sources" not in config:
        return False, "missing sources key in config"
    if "outputs" not in config:
        return False, "missing outputs key in config"
    for table in all_tables:
        if table not in config["tables"]:
            return False, f"missing {table} in tables"
        if config["tables"][table] and table not in config["sources"]:
            return False, f"missing {table} in sources"
        if config["tables"][table] and table not in config["outputs"]:
            return False, f"missing {table} in outputs"
    if "raw_detection" not in config["sources"]:
        return False, "missing raw_detection in sources"
    if "csv_loader_config" not in config:
        return False, "missing csv_loader_config"
    for opt in csv_loader_options:
        if opt not in config["csv_loader_config"]:
            return False, f"missing {opt} in config"
    return True, None


def create_and_upload_csv(Loader, table_name, config, session, default_args, **kwargs):
    loader = Loader(source=config["sources"][table_name], read_args=default_args)
    loader.save_csv(
        spark_session=session,
        output_path=config["outputs"][table_name],
        n_partitions=config["csv_loader_config"]["n_partitions"],
        max_records_per_file=config["csv_loader_config"]["max_records_per_file"],
        mode=config["csv_loader_config"]["mode"],
        **kwargs,
    )
    loader.psql_load_csv(config["outputs"][table_name], config["db"])


@click.command()
@click.argument("config_file")
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def process_csv(config_file, loglevel):
    if not os.path.exists(config_file):
        raise Exception("Config file not found")
    sys.path.append(os.path.dirname(os.path.expanduser(config_file)))
    from config import load_config as config

    valid, message = validate_config(config)
    if not valid:
        raise Exception(message)
    spark = create_session()
    default_args = {}
    tt_det = get_tt_det(
        spark, config["sources"]["detection"], config["sources"]["raw_detection"]
    )
    step_id = "bulk_1.0.0"
    obj_cid_window = Window.partitionBy("objectId").orderBy("candid")
    if config["tables"]["detection"]:
        create_and_upload_csv(
            DetectionsCSVLoader,
            "detection",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            step_id=step_id,
        )
    if config["tables"]["object"]:
        create_and_upload_csv(
            ObjectsCSVLoader,
            "object",
            config,
            spark,
            default_args,
        )
    if config["tables"]["non_detection"]:
        create_and_upload_csv(
            NonDetectionsCSVLoader,
            "non_detection",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ss"]:
        create_and_upload_csv(
            SSCSVLoader,
            "ss",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            obj_cid_window=obj_cid_window,
        )

    if config["tables"]["magstats"]:
        create_and_upload_csv(
            MagstatsCSVLoader,
            "magstats",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ps1"]:
        create_and_upload_csv(
            PS1CSVLoader,
            "ps1",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
            fun=min,
        )
    if config["tables"]["reference"]:
        create_and_upload_csv(
            ReferenceCSVLoader,
            "reference",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            fun=min,
        )

    if config["tables"]["dataquality"]:
        create_and_upload_csv(
            DataQualityCSVLoader,
            "dataquality",
            config,
            spark,
            default_args,
            tt_det=tt_det,
        )
    if config["tables"]["xmatch"]:
        pass
    if config["tables"]["probability"]:
        pass
    if config["tables"]["feature"]:
        pass
