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
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
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
        "ps1_ztf",
        "gaia_ztf",
        "ss_ztf",
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


def loader_save_and_upload(Loader, table_name, config, session, default_args, **kwargs):
    loader = Loader(source=config["sources"][table_name], read_args=default_args)
    loader.save_csv(
        spark_session=session,
        output_path=config["outputs"][table_name],
        n_partitions=config["csv_loader_config"]["n_partitions"],
        max_records_per_file=config["csv_loader_config"]["max_records_per_file"],
        mode=config["csv_loader_config"]["mode"],
        **kwargs,
    )
    loader.psql_load_csv(config["outputs"][table_name], config["db"], table_name)


def loader_create_csv(Loader, table_name, config, session, default_args, **kwargs):
    loader = Loader(source=config["sources"][table_name], read_args=default_args)
    loader.save_csv(
        spark_session=session,
        output_path=config["outputs"][table_name],
        n_partitions=config["csv_loader_config"]["n_partitions"],
        max_records_per_file=config["csv_loader_config"]["max_records_per_file"],
        mode=config["csv_loader_config"]["mode"],
        **kwargs,
    )


def loader_load_csv(Loader, table_name, config, session, default_args, **kwargs):
    Loader.psql_load_csv(config["outputs"][table_name], config["db"], table_name)


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
    """
    Creates and uploads CSV files from source parquet to a PSQL database. The following arguments are required:

    CONFIG_FILE: The path to a valid config.py file
    """
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
        loader_save_and_upload(
            DetectionsCSVLoader,
            "detection",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            step_id=step_id,
        )
    if config["tables"]["object"]:
        loader_save_and_upload(
            ObjectsCSVLoader,
            "object",
            config,
            spark,
            default_args,
        )
    if config["tables"]["non_detection"]:
        loader_save_and_upload(
            NonDetectionsCSVLoader,
            "non_detection",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ss_ztf"]:
        loader_save_and_upload(
            SSCSVLoader,
            "ss_ztf",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            obj_cid_window=obj_cid_window,
        )

    if config["tables"]["magstats"]:
        loader_save_and_upload(
            MagstatsCSVLoader,
            "magstats",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ps1_ztf"]:
        loader_save_and_upload(
            PS1CSVLoader,
            "ps1_ztf",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
        )
    if config["tables"]["gaia_ztf"]:
        loader_save_and_upload(
            PS1CSVLoader,
            "gaia_ztf",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
        )
    if config["tables"]["reference"]:
        loader_save_and_upload(
            ReferenceCSVLoader,
            "reference",
            config,
            spark,
            default_args,
            tt_det=tt_det,
        )

    if config["tables"]["dataquality"]:
        loader_save_and_upload(
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


@click.command()
@click.argument("config_file")
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def create_csv(config_file, loglevel):
    """
    Creates CSV files from source parquet. The following arguments are required:

    CONFIG_FILE: The path to a valid config.py file
    """
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
        loader_create_csv(
            DetectionsCSVLoader,
            "detection",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            step_id=step_id,
        )
    if config["tables"]["object"]:
        loader_create_csv(
            ObjectsCSVLoader,
            "object",
            config,
            spark,
            default_args,
        )
    if config["tables"]["non_detection"]:
        loader_create_csv(
            NonDetectionsCSVLoader,
            "non_detection",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ss_ztf"]:
        loader_create_csv(
            SSCSVLoader,
            "ss_ztf",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            obj_cid_window=obj_cid_window,
        )

    if config["tables"]["magstats"]:
        loader_create_csv(
            MagstatsCSVLoader,
            "magstats",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ps1_ztf"]:
        loader_create_csv(
            PS1CSVLoader,
            "ps1_ztf",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
            fun=min,
        )
    if config["tables"]["gaia_ztf"]:
        loader_create_csv(
            PS1CSVLoader,
            "gaia_ztf",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
            fun=min,
        )
    if config["tables"]["reference"]:
        loader_create_csv(
            ReferenceCSVLoader,
            "reference",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            fun=min,
        )

    if config["tables"]["dataquality"]:
        loader_create_csv(
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


@click.command()
@click.argument("config_file")
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def psql_copy_csv(config_file, loglevel):
    """
    Uploads CSV files to a PSQL database. The following arguments are required:

    CONFIG_FILE: The path to a valid config.py file
    """
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
        loader_load_csv(
            DetectionsCSVLoader,
            "detection",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            step_id=step_id,
        )
    if config["tables"]["object"]:
        loader_load_csv(
            ObjectsCSVLoader,
            "object",
            config,
            spark,
            default_args,
        )
    if config["tables"]["non_detection"]:
        loader_load_csv(
            NonDetectionsCSVLoader,
            "non_detection",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ss_ztf"]:
        loader_load_csv(
            SSCSVLoader,
            "ss_ztf",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            obj_cid_window=obj_cid_window,
        )

    if config["tables"]["magstats"]:
        loader_load_csv(
            MagstatsCSVLoader,
            "magstats",
            config,
            spark,
            default_args,
        )
    if config["tables"]["ps1_ztf"]:
        loader_load_csv(
            PS1CSVLoader,
            "ps1_ztf",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
            fun=min,
        )
    if config["tables"]["gaia_ztf"]:
        loader_load_csv(
            PS1CSVLoader,
            "gaia_ztf",
            config,
            spark,
            default_args,
            obj_cid_window=obj_cid_window,
            fun=min,
        )
    if config["tables"]["reference"]:
        loader_load_csv(
            ReferenceCSVLoader,
            "reference",
            config,
            spark,
            default_args,
            tt_det=tt_det,
            fun=min,
        )

    if config["tables"]["dataquality"]:
        loader_load_csv(
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
