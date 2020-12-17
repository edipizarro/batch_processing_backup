import click
import time
import logging
from table_columns import (
    det_col,
    obj_col,
    non_col,
    ss_col,
    qua_col,
    mag_col,
    ps1_col,
    gaia_col,
    ref_col,
    xch_col,
)

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import (
    col,
    lit,
    countDistinct,
    explode,
    array,
    struct,
    when,
    substring,
    expr,
    split,
    dense_rank,
)

DATA_SOURCES = {
    "dfc": "s3a://ztf-historic-data/det",
    "det": "s3a://ztf-historic-data/detection_corr",
    "obj": "s3a://ztf-historic-data/object",
    "non": "s3a://ztf-historic-data/non_detection",
    "mag": "s3a://ztf-historic-data/magstats",
    "xch": "s3a://ztfxmatch/ztfxallwise_20200626.parquet",
    "fea": "s3a://ztf-historic-data/feature/XQom3",
    "prob": "s3a://ztf-historic-data/probability/abult",
}


def create_session():
    logging.info("Creating spark session")
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_dataframes():
    logging.info("Reading data sources")
    dataframes = {}
    for key in DATA_SOURCES.keys():
        logging.debug(f"Reading data for {key} at {DATA_SOURCES[key]}")
        kwargs = {}
        if key == "prob":
            kwargs["format"] = "csv"
            kwargs["inferSchgema"] = "true"
            kwargs["header"] = "true"
        if key == "fea":
            kwargs["header"] = "true"
        dataframes[key] = spark.read.load(DATA_SOURCES[key], **kwargs)
    return dataframes


def select_detections(df_det, tt_det):
    logging.info("Processing detections")
    data_det = (
        tt_det.select(
            "i.aimage",
            "i.aimagerat",
            "i.bimage",
            "i.bimagerat",
            "i.candid",
            "i.chinr",
            "i.chipsf",
            "i.classtar",
            "i.corrected",
            "i.dec",
            "i.decnr",
            "i.diffmaglim",
            "i.distnr",
            "i.distpsnr1",
            "i.distpsnr2",
            "i.distpsnr3",
            "i.dubious",
            "i.elong",
            "i.fid",
            "i.field",
            "i.fwhm",
            df_det.isdiffpos.cast(IntegerType()),
            "i.jdendhist",
            "i.jdendref",
            "i.jdstarthist",
            "i.jdstartref",
            "i.magap",
            "i.magapbig",
            "i.magdiff",
            "i.magfromlim",
            "i.magnr",
            "i.magpsf",
            "i.magpsf_corr",
            "i.mindtoedge",
            "i.mjd",
            "i.nbad",
            "i.ncovhist",
            "i.ndethist",
            "i.nframesref",
            "i.nid",
            "i.nmtchps",
            "i.nneg",
            "i.objectId",
            "i.objectidps1",
            "i.objectidps2",
            "i.objectidps3",
            "i.parent_candid",
            "i.pdiffimfilename",
            "i.pid",
            "i.programid",
            "i.programpi",
            "i.ra",
            "i.ranr",
            "i.rb",
            "i.rcid",
            "i.rfid",
            "i.scorr",
            "i.seeratio",
            "i.sgmag1",
            "i.sgmag2",
            "i.sgmag3",
            "i.sgscore1",
            "i.sgscore2",
            "i.sgscore3",
            "i.sharpnr",
            "i.sigmagap",
            "i.sigmagapbig",
            "i.sigmagnr",
            "i.sigmapsf",
            "i.sigmapsf_corr",
            "i.sigmapsf_corr_ext",
            "i.simag1",
            "i.simag2",
            "i.simag3",
            "i.sky",
            "i.srmag1",
            "i.srmag2",
            "i.srmag3",
            "i.ssdistnr",
            "i.ssmagnr",
            "i.ssnamenr",
            "i.sumrat",
            "i.szmag1",
            "i.szmag2",
            "i.szmag3",
            "i.tblid",
            "i.tooflag",
            "i.xpos",
            "i.ypos",
            "rbversion",
            "drb",
            "drbversion",
        )
        .withColumn("has_stamp", col("parent_candid") == 0)
        .withColumn("step_id_corr", lit("corr_bulk_0.0.1"))
    )

    data_det = data_det.fillna("", "rbversion")
    data_det = data_det.fillna("", "drbversion")

    sel_det = data_det.select(*[col(c) for c in det_col])
    return sel_det


def select_objects(df_obj):
    logging.info("Processing objects")
    obj_col.remove("objectId")
    obj_col.remove("ndethist")
    obj_col.remove("ncovhist")

    sel_obj = df_obj.select(
        "objectId",
        df_obj.ndethist.cast(IntegerType()),
        df_obj.ncovhist.cast(IntegerType()),
        *[col(c) for c in obj_col],
    )
    return sel_obj


def select_non_det(df_non):
    logging.info("Processing non detections")
    tt_non = df_non.withColumn("mjd", col("jd") - 2400000.5).drop("jd")
    sel_non = tt_non.select(*[col(c) for c in non_col])
    return sel_non


def select_ss(tt_det):
    logging.info("Processing ss")
    ss_col.remove("objectId")
    ss_col.remove("candid")
    tt_ss = tt_det.select(
        "objectId", "candid", *[col("c." + c).alias(c) for c in ss_col]
    )
    obj_cid_window = Window.partitionBy("objectId").orderBy("candid")
    tt_ss_min = (
        tt_ss.withColumn("mincandid", min(col("candid")).over(obj_cid_window))
        .where(col("candid") == col("mincandid"))
        .select("objectId", "candid", *ss_col)
    )
    return tt_ss_min, obj_cid_window


def select_dataquality(tt_det):
    logging.info("Processing dataquality")
    qua_col.remove("objectId")
    qua_col.remove("candid")
    tt_qua = tt_det.select(
        "objectId", "candid", *[col("c." + c).alias(c) for c in qua_col]
    )
    return tt_qua


def select_magstats(df_mag):
    logging.info("Processing magstats")
    data_mag = df_mag.withColumn("magsigma", lit("")).withColumn(
        "magsigma_corr", lit("")
    )
    sel_mag = data_mag.select(*[col(c) for c in mag_col])
    return sel_mag


def select_ps1(df_det, obj_cid_window):
    logging.info("Processing ps1")
    ps1_col.remove("objectId")
    ps1_col.remove("unique1")
    ps1_col.remove("unique2")
    ps1_col.remove("unique3")

    tt_ps1 = df_det.select("objectId", *[col(c) for c in ps1_col])

    tt_ps1_min = (
        tt_ps1.withColumn("mincandid", min(col("candid")).over(obj_cid_window))
        .where(col("candid") == col("mincandid"))
        .select("objectId", *ps1_col)
    )

    data_ps1 = (
        tt_ps1_min.alias("i")
        .join(tt_ps1.alias("c"), "objectId", "inner")
        .select(
            "objectId",
            col("i.objectidps1").alias("min_objectidps1"),
            col("i.objectidps2").alias("min_objectidps2"),
            col("i.objectidps3").alias("min_objectidps3"),
            *[col("i." + c).alias(c) for c in ps1_col],
        )
        .withColumn("unique1", col("min_objectidps1") != col("objectidps1"))
        .withColumn("unique2", col("min_objectidps2") != col("objectidps2"))
        .withColumn("unique3", col("min_objectidps3") != col("objectidps3"))
        .drop("min_objectidps1")
        .drop("min_objectidps2")
        .drop("min_objectidps3")
    )

    gr_ps1 = (
        data_ps1.groupBy("objectId", *ps1_col)
        .agg(
            countDistinct("unique1").alias("count1"),
            countDistinct("unique2").alias("count2"),
            countDistinct("unique3").alias("count3"),
        )
        .withColumn("unique1", col("count1") != 1)
        .withColumn("unique2", col("count2") != 1)
        .withColumn("unique3", col("count3") != 1)
        .drop("count1")
        .drop("count2")
        .drop("count3")
    )

    return gr_ps1


def select_gaia(tt_det, obj_cid_window, real_threshold):
    logging.info("Processing gaia")
    gaia_col.remove("objectId")
    gaia_col.remove("candid")
    gaia_col.remove("unique1")

    tt_gaia = tt_det.select(
        "objectId", "candid", *[col("c." + c).alias(c) for c in gaia_col]
    )

    tt_gaia_min = (
        tt_gaia.withColumn("mincandid", min(col("candid")).over(obj_cid_window))
        .where(col("candid") == col("mincandid"))
        .select("objectId", "candid", *gaia_col)
    )

    data_gaia = (
        tt_gaia_min.alias("i")
        .join(tt_gaia.alias("c"), "objectId", "inner")
        .select(
            "objectId",
            "i.candid",
            col("i.maggaia").alias("min_maggaia"),
            *[col("i." + c).alias(c) for c in gaia_col],
        )
        .withColumn(
            "unique1", abs(col("min_maggaia") - col("maggaia")) > real_threshold
        )
        .drop("min_maggaia")
    )

    gr_gaia = (
        data_gaia.groupBy("objectId", "candid", *gaia_col)
        .agg(countDistinct("unique1").alias("count1"))
        .withColumn("unique1", col("count1") != 1)
        .drop("count1")
    )

    return gr_gaia


def select_ref(df_det):
    logging.info("Processing reference")
    ref_col.remove("objectId")
    ref_col.remove("rfid")

    tt_ref = df_det.select("rfid", "objectId", *[col(c) for c in ref_col])

    obj_rfid_cid_window = Window.partitionBy("objectId", "rfid").orderBy("candid")
    tt_ref_min = (
        tt_ref.withColumn("mincandid", min(col("candid")).over(obj_rfid_cid_window))
        .where(col("candid") == col("mincandid"))
        .select("rfid", "objectId", *ref_col)
    )
    return tt_ref_min


def select_xmatch(df_xch):
    logging.info("Processing xmatch")
    selection = (
        df_xch.select(xch_col)
        .withColumnRenamed("objectId_2", "oid")
        .withColumnRenamed("designation", "oid_catalog")
    )

    # allwise data
    allwise = selection.drop("oid")

    # xmatch data
    xmatch = selection.select("oid_catalog", "oid").withColumn("catid", lit("allwise"))
    return xmatch, allwise


def select_features(df_fea):
    logging.info("Processing features")
    cols, dtypes = zip(*((c, t) for (c, t) in df_fea.dtypes if c not in ["oid"]))
    kvs = explode(
        array([struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])
    ).alias("kvs")
    df2_fea = (
        df_fea.select(["oid"] + [kvs])
        .select(["oid"] + ["kvs.key", "kvs.value"])
        .withColumn(
            "fid",
            when(
                (col("key") == "gal_b")
                | (col("key") == "gal_l")
                | (col("key") == "rb")
                | (col("key") == "sgscore1")
                | (col("key") == "W1")
                | (col("key") == "W2")
                | (col("key") == "W3")
                | (col("key") == "W4")
                | (col("key") == "W1-W2")
                | (col("key") == "W2-W3")
                | (col("key") == "r-W3")
                | (col("key") == "r-W2")
                | (col("key") == "g-W3")
                | (col("key") == "g-W2")
                | (col("key") == "g-r_ml"),
                0,
            )
            .when(
                (col("key") == "g-r_max")
                | (col("key") == "g-r_max_corr")
                | (col("key") == "g-r_mean")
                | (col("key") == "g-r_mean_corr")
                | (col("key") == "Multiband_period")
                | (col("key") == "Period_fit"),
                12,
            )
            .otherwise(substring("key", -1, 1)),
        )
        .withColumn(
            "name",
            when((col("fid") == 0) | (col("fid") == 12), (col("key"))).otherwise(
                expr("substring(key, 1, length(key)-2)")
            ),
        )
        .withColumn("version", lit("bulk_0.0.1"))
        .drop("key")
    )

    df3_fea = df2_fea.withColumn(
        "value", when((col("value").like("%E%")), 0).otherwise(col("value"))
    )
    sel_fea = df3_fea.select("oid", "name", "value", "fid", "version")
    return sel_fea


def select_prob(df_prob):
    logging.info("Processing probabilities")
    cols_p, dtypes_p = zip(*((c, t) for (c, t) in df_prob.dtypes if c not in ["oid"]))
    kvs_p = explode(
        array([struct(lit(c).alias("key"), col(c).alias("value")) for c in cols_p])
    ).alias("kvs")
    df2_prob = (
        df_prob.select(["oid"] + [kvs_p])
        .select(["oid"] + ["kvs.key", "kvs.value"])
        .withColumn("classifier_version", lit("bulk_0.0.1"))
        .withColumn("class_name", (split(col("key"), "_").getItem(0)))
        .withColumn("probability", (col("value")))
    )
    df3_prob = df2_prob.filter(
        (col("key") != "predicted_class") & (col("key") != "predicted_class_proba")
    )
    df4_prob = df3_prob.withColumn(
        "classifier_name",
        when((substring(col("key"), -3, 3) == "top"), "lc_classifier_top")
        .when((substring(col("key"), -1, 1) == "T"), "lc_classifier_transient")
        .when((substring(col("key"), -1, 1) == "S"), "lc_classifier_stochastic")
        .when((substring(col("key"), -1, 1) == "P"), "lc_classifier_periodic")
        .when((substring(col("key"), -4, 4) == "prob"), "lc_classifier")
        .otherwise("NaN"),
    ).select(
        df3_prob.probability.cast(DoubleType()),
        "oid",
        "classifier_name",
        "classifier_version",
        "class_name",
        "value",
        "key",
    )
    df5_prob = df4_prob.withColumn(
        "ranking",
        dense_rank().over(
            Window.partitionBy("oid", "classifier_name").orderBy(desc("probability"))
        ),
    )
    df6_prob = df5_prob.drop("key").drop("value")
    sel_prob = df6_prob.select(
        "oid",
        "classifier_name",
        "classifier_version",
        "class_name",
        "probability",
        "ranking",
    )
    return sel_prob


def save_det(sel_det, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing detections")
    sel_det.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "detection", emptyValue="")


def save_obj(sel_obj, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing objects")
    sel_obj.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "object", emptyValue="")


def save_non_det(sel_non, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing non detections")
    sel_non.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "non_detection", emptyValue="")


def save_ss(tt_ss_min, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing ss")
    tt_ss_min.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "ss_ztf", emptyValue="")


def save_dataquality(tt_qua, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing dataquality")
    tt_qua.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "dataquality", emptyValue="")


def save_magstats(sel_mag, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing magstats")
    sel_mag.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "magstats", emptyValue="")


def save_ps1(gr_ps1, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing ps1")
    gr_ps1.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "ps1_ztf", emptyValue="")


def save_gaia(gr_gaia, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing gaia")
    gr_gaia.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "gaia_ztf", emptyValue="")


def save_reference(tt_ref_min, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing reference")
    tt_ref_min.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "reference", emptyValue="")


def save_allwise(allwise, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing allwise")
    allwise.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "allwise", emptyValue="")


def save_xmatch(xmatch, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing xmatch")
    xmatch.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "xmatch", emptyValue="")


def save_features(sel_fea, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing features")
    sel_fea.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "feature", emptyValue="")


def save_prob(sel_prob, n_partitions, max_records_per_file, mode, output_dir):
    logging.info("Writing probabilities")
    sel_prob.coalesce(n_partitions).write.option(
        "maxRecordsPerFile", max_records_per_file
    ).mode(mode).csv(output_dir + "probabilities", emptyValue="")


@click.command()
@click.argument("output_path")
@click.option("--save-mode", default="overwrite", help="Write mode for pyspark")
@click.option(
    "--max-records-per-file", default=100000, help="Max number of rows in each CSV"
)
@click.option("--npartitions", default=16, help="Number of dataframe partitions")
@click.option("--real-threshold", default=1e-4, help="Threshold to filter gaia")
def main(output_path, save_mode, max_records_per_file, npartitions, real_threshold):
    """
    OUTPUT_PATH is the name of the path to store CSV files. OUTPUT_PATH can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/
    """
    # Set logging level
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(filename="fill_missing_fields.log", level=numeric_level)

    # Track time of the process
    start = time.time()
    # Pre configuration
    spark = create_session()
    # Read data sources
    dfs = read_dataframes()
    # Process data
    tt_det = (
        dfs["det"]
        .alias("i")
        .join(dfs["dfc"].alias("c"), ["objectId", "candid"], "inner")
    )
    sel_det = select_detections(dfs["det"], tt_det)
    sel_obj = select_objects(dfs["obj"])
    sel_non = select_non_det(dfs["non"])
    tt_ss_min, obj_cid_window = select_ss(tt_det)
    tt_qua = select_dataquality(tt_det)
    sel_mag = select_magstats(dfs["mag"])
    xmatch, allwise = select_xmatch(dfs["xch"])
    gr_ps1 = select_ps1(dfs["det"], obj_cid_window)
    gr_gaia = select_gaia(tt_det, obj_cid_window, real_threshold)
    tt_ref_min = select_ref(dfs["det"])
    sel_fea = select_features(dfs["fea"])
    sel_prob = select_prob(dfs["prob"])
    # Write csv
    kwargs = {
        "n_partitions": npartitions,
        "max_records_per_file": max_records_per_file,
        "mode": save_mode,
        "output_dir": output_path,
    }
    save_det(sel_det, **kwargs)
    save_obj(sel_obj, **kwargs)
    save_non_det(sel_non, **kwargs)
    save_ss(tt_ss_min, **kwargs)
    save_dataquality(tt_qua, **kwargs)
    save_magstats(sel_mag, **kwargs)
    save_ps1(gr_ps1, **kwargs)
    save_gaia(gr_gaia, **kwargs)
    save_reference(tt_ref_min, **kwargs)
    save_allwise(allwise, **kwargs)
    save_xmatch(xmatch, **kwargs)
    save_features(sel_fea, **kwargs)
    save_prob(sel_prob, **kwargs)

    # Get elapsed time
    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))
    # Log finish
    logging.info("Exit")


if __name__ == "__main__":
    main()
