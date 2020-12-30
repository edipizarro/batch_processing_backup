from load_psql.loaders import (
    DetectionsCSVLoader,
    NonDetectionsCSVLoader,
    ObjectsCSVLoader,
    SSCSVLoader,
)


def create_session():
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    # logging.info("Creating spark session")
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_loader_config(table, config, default_config):
    if table in config:
        return default_config.update(config[table])
    else:
        return default_config


def get_tt_det():
    pass


def create_csv(sources, outputs, config=None):
    spark = create_session()
    default_args = {}
    default_config = {}  # add default config
    config = config or {}
    tt_det = get_tt_det()
    if "detections" in sources and "detections" in outputs:
        detections_loader = DetectionsCSVLoader(
            sources["detections"], read_args=default_args
        )
        loader_config = get_loader_config("detections", config, default_config)
        step_id = "bulk_1.0.0"
        detections_loader.save_csv(
            spark_session=spark,
            output_path=outputs["detections"],
            n_partitions=loader_config["n_partitions"],
            max_records_per_file=loader_config["max_records_per_file"],
            mode=loader_config["mode"],
            tt_det=tt_det,
            step_id=step_id,
        )
        detections_loader.psql_load_csv()

    if "objects" in sources and "objects" in outputs:
        objects_loader = ObjectsCSVLoader(
            source=sources["objects"], read_args=default_args
        )
        loader_config = get_loader_config("objects", config, default_config)
        objects_loader.save_csv(
            spark_session=spark,
            output_path=outputs["detections"],
            n_partitions=loader_config["n_partitions"],
            max_records_per_file=loader_config["max_records_per_file"],
            mode=loader_config["mode"],
        )
        objects_loader.psql_load_csv()
