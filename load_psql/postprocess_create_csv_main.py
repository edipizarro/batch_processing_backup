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
    XmatchCSVLoader,
    AllwiseCSVLoader,
    FeatureCSVLoader,
)
from load_psql.table_data.table_columns import (
    det_col,
    non_col,
    obj_col,
    mag_col,
    qua_col,
    ps1_col,
    ss_col,
    gaia_col,
    ref_col,
    xmatch_col,
    allwise_col,
    fea_col,
)

from pyspark.sql import SparkSession, Window
from pyspark import SparkConf, SparkContext
import click
import os
import json


def create_session(spark_driver_memory=None, spark_local_dir=None):

    # logging.info("Creating spark session")
    conf = SparkConf()
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2")
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    if spark_driver_memory:
        conf.set("spark.driver.memory", spark_driver_memory)

    if spark_local_dir:
        conf.set("spark.local.dir", spark_local_dir)
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
        "magstat",
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


def loader_create_csv(
    Loader, table_name, config, session, default_args, column_list, **kwargs
):
    loader = Loader(source=config["sources"][table_name], read_args=default_args)
    loader.save_csv(
        spark_session=session,
        output_path=config["outputs"][table_name],
        n_partitions=config["csv_loader_config"]["n_partitions"],
        max_records_per_file=config["csv_loader_config"]["max_records_per_file"],
        mode=config["csv_loader_config"]["mode"],
        column_list=column_list,
        **kwargs,
    )


def loader_load_csv(Loader, table_name, config):
    Loader.psql_load_csv(config["outputs"][table_name], config["db"], table_name)


def get_config_from_file(path: str) -> dict:
    if not os.path.exists(path):
        raise Exception("Config file not found")
    with open(path) as f:
        data = json.load(f)
        return data["load_psql_config"]


def get_config_from_str(config: str) -> dict:
    data = json.loads(config)
    return data


@click.command()
@click.option(
    "--config_file",
    "config_file",
    help="use a json file for configuration",
    type=str,
)
@click.option(
    "--config_json", "config_json", help="use a json string for configuration", type=str
)
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def process_csv(
    config_file, config_json, loglevel, spark_driver_memory=None, spark_local_dir=None
):
    """
    Creates and uploads CSV files from source parquet to a PSQL database.


    """
    if config_file:
        config = get_config_from_file(config_file)
    elif config_json:
        config = get_config_from_str(config_json)
    else:
        raise Exception("Provide at least one source for configuration")
    valid, message = validate_config(config)
    if not valid:
        raise Exception(message)
    spark_driver_memory = config.get("spark.driver.memory", None)
    spark_local_dir = config.get("spark.local.dir", None)
    spark = create_session(spark_driver_memory, spark_local_dir)
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
            det_col.copy(),
            tt_det=tt_det,
            step_id=step_id,
        )
        loader_load_csv(DetectionsCSVLoader, "detection", config)
    if config["tables"]["object"]:
        loader_create_csv(
            ObjectsCSVLoader, "object", config, spark, default_args, obj_col.copy()
        )
        loader_load_csv(ObjectsCSVLoader, "object", config)
    if config["tables"]["non_detection"]:
        loader_create_csv(
            NonDetectionsCSVLoader,
            "non_detection",
            config,
            spark,
            default_args,
            non_col.copy(),
        )
        loader_load_csv(NonDetectionsCSVLoader, "non_detection", config)
    if config["tables"]["ss_ztf"]:
        loader_create_csv(
            SSCSVLoader,
            "ss_ztf",
            config,
            spark,
            default_args,
            ss_col.copy(),
            tt_det=tt_det,
            obj_cid_window=obj_cid_window,
        )
        loader_load_csv(SSCSVLoader, "ss_ztf", config)

    if config["tables"]["magstat"]:
        loader_create_csv(
            MagstatsCSVLoader, "magstat", config, spark, default_args, mag_col.copy()
        )
        loader_load_csv(MagstatsCSVLoader, "magstat", config)
    if config["tables"]["ps1_ztf"]:
        loader_create_csv(
            PS1CSVLoader,
            "ps1_ztf",
            config,
            spark,
            default_args,
            ps1_col.copy(),
            obj_cid_window=obj_cid_window,
        )
        loader_load_csv(PS1CSVLoader, "ps1_ztf", config)
    if config["tables"]["gaia_ztf"]:
        loader_create_csv(
            GaiaCSVLoader,
            "gaia_ztf",
            config,
            spark,
            default_args,
            gaia_col.copy(),
            obj_cid_window=obj_cid_window,
        )
        loader_load_csv(GaiaCSVLoader, "gaia_ztf", config)
    if config["tables"]["reference"]:
        loader_create_csv(
            ReferenceCSVLoader,
            "reference",
            config,
            spark,
            default_args,
            ref_col.copy(),
            tt_det=tt_det,
        )
        loader_load_csv(ReferenceCSVLoader, "reference", config)

    if config["tables"]["dataquality"]:
        loader_create_csv(
            DataQualityCSVLoader,
            "dataquality",
            config,
            spark,
            default_args,
            qua_col.copy(),
            tt_det=tt_det,
        )
        loader_load_csv(DataQualityCSVLoader, "dataquality", config)
    if config["tables"]["xmatch"]:
        loader_create_csv(
            XmatchCSVLoader, "xmatch", config, spark, default_args, xmatch_col.copy()
        )
        loader_load_csv(XmatchCSVLoader, "xmatch", config)

    if config["tables"]["allwise"]:
        loader_create_csv(
            AllwiseCSVLoader, "allwise", config, spark, default_args, allwise_col.copy()
        )
        loader_load_csv(AllwiseCSVLoader, "allwise", config)

    if config["tables"]["probability"]:
        pass
    if config["tables"]["feature"]:
        version = config.get("feature_version", "-")
        loader_create_csv(
            FeatureCSVLoader,
            "feature",
            config,
            spark,
            default_args,
            fea_col.copy(),
            version=version,
        )
        loader_load_csv(FeatureCSVLoader, "feature", config)


@click.command()
@click.option(
    "--config_file",
    "config_file",
    help="use a json file for configuration",
    type=str,
)
@click.option(
    "--config_json", "config_json", help="use a json string for configuration", type=str
)
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def create_csv(config_file, config_json, loglevel):
    """
    Creates CSV files from source parquet.


    """
    if config_file:
        config = get_config_from_file(config_file)
    elif config_json:
        config = get_config_from_str(config_json)
    else:
        raise Exception("Provide at least one source for configuration")
    valid, message = validate_config(config)
    if not valid:
        raise Exception(message)
    spark_driver_memory = config.get("spark.driver.memory", None)
    spark_local_dir = config.get("spark.local.dir", None)
    spark = create_session(spark_driver_memory, spark_local_dir)
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
            det_col.copy(),
            tt_det=tt_det,
            step_id=step_id,
        )
    if config["tables"]["object"]:
        loader_create_csv(
            ObjectsCSVLoader, "object", config, spark, default_args, obj_col.copy()
        )
    if config["tables"]["non_detection"]:
        loader_create_csv(
            NonDetectionsCSVLoader,
            "non_detection",
            config,
            spark,
            default_args,
            non_col.copy(),
        )
    if config["tables"]["ss_ztf"]:
        loader_create_csv(
            SSCSVLoader,
            "ss_ztf",
            config,
            spark,
            default_args,
            ss_col.copy(),
            tt_det=tt_det,
            obj_cid_window=obj_cid_window,
        )

    if config["tables"]["magstat"]:
        loader_create_csv(
            MagstatsCSVLoader, "magstat", config, spark, default_args, mag_col.copy()
        )
    if config["tables"]["ps1_ztf"]:
        loader_create_csv(
            PS1CSVLoader,
            "ps1_ztf",
            config,
            spark,
            default_args,
            ps1_col.copy(),
            obj_cid_window=obj_cid_window,
        )
    if config["tables"]["gaia_ztf"]:
        loader_create_csv(
            GaiaCSVLoader,
            "gaia_ztf",
            config,
            spark,
            default_args,
            gaia_col.copy(),
            tt_det=tt_det,
            real_threshold=1e-4,
            obj_cid_window=obj_cid_window,
        )
    if config["tables"]["reference"]:
        loader_create_csv(
            ReferenceCSVLoader,
            "reference",
            config,
            spark,
            default_args,
            ref_col.copy(),
            tt_det=tt_det,
        )

    if config["tables"]["dataquality"]:
        loader_create_csv(
            DataQualityCSVLoader,
            "dataquality",
            config,
            spark,
            default_args,
            qua_col.copy(),
            tt_det=tt_det,
        )
    if config["tables"]["xmatch"]:
        loader_create_csv(
            XmatchCSVLoader, "xmatch", config, spark, default_args, xmatch_col.copy()
        )

    if config["tables"]["allwise"]:
        loader_create_csv(
            AllwiseCSVLoader, "allwise", config, spark, default_args, allwise_col.copy()
        )

    if config["tables"]["probability"]:
        pass
    if config["tables"]["feature"]:
        version = config.get("feature_version", "-")
        loader_create_csv(
            FeatureCSVLoader,
            "feature",
            config,
            spark,
            default_args,
            fea_col.copy(),
            version=version,
        )


@click.command()
@click.option(
    "--config_file",
    "config_file",
    help="use a json file for configuration",
    type=str,
)
@click.option(
    "--config_json", "config_json", help="use a json string for configuration", type=str
)
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def psql_copy_csv(
    config_file, config_json, loglevel, spark_driver_memory=None, spark_local_dir=None
):
    """
    Uploads CSV files to a PSQL database.
    """
    if config_file:
        config = get_config_from_file(config_file)
    elif config_json:
        config = get_config_from_str(config_json)
    else:
        raise Exception("Provide at least one source for configuration")

    valid, message = validate_config(config)
    if not valid:
        raise Exception(message)
    default_args = {}

    if config["tables"]["detection"]:
        loader_load_csv(DetectionsCSVLoader, "detection", config)
    if config["tables"]["object"]:
        loader_load_csv(ObjectsCSVLoader, "object", config)
    if config["tables"]["non_detection"]:
        loader_load_csv(NonDetectionsCSVLoader, "non_detection", config)
    if config["tables"]["ss_ztf"]:
        loader_load_csv(SSCSVLoader, "ss_ztf", config)

    if config["tables"]["magstat"]:
        loader_load_csv(MagstatsCSVLoader, "magstat", config)
    if config["tables"]["ps1_ztf"]:
        loader_load_csv(PS1CSVLoader, "ps1_ztf", config)
    if config["tables"]["gaia_ztf"]:
        loader_load_csv(GaiaCSVLoader, "gaia_ztf", config)
    if config["tables"]["reference"]:
        loader_load_csv(ReferenceCSVLoader, "reference", config)
    if config["tables"]["dataquality"]:
        loader_load_csv(DataQualityCSVLoader, "dataquality", config)
    if config["tables"]["xmatch"]:
        loader_load_csv(XmatchCSVLoader, "xmatch", config)
    if config["tables"]["allwise"]:
        loader_load_csv(AllwiseCSVLoader, "allwise", config)
    if config["tables"]["probability"]:
        pass
    if config["tables"]["feature"]:
        loader_load_csv(FeatureCSVLoader, "feature", config)
