import click
import time
import logging


LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


@click.command()
@click.argument("input_features", type=str)
@click.argument("input_wise", type=str)
@click.argument("input_sgscore", type=str)
@click.argument("output_features", type=str)
@click.option("--n-partitions", "-n", default=300, help="Number of output parquet files")
@click.option("--log", "loglevel", default="INFO", help="log level to use", type=click.Choice(LOG_LEVELS))
def join_data(
        input_features,
        input_wise,
        input_sgscore,
        output_features,
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
    # 1) features
    df_features = spark.read.load(input_features)
    # 2) wise
    w_cols = ['source_id','ra','dec','w1mpro','w1sigmpro','w2mpro','w2sigmpro','w3mpro','w3sigmpro','w4mpro','w4sigmpro','oid','ra_2','dec_2','distance']
    df_wise = spark.read.option("header", False).csv(input_wise)
    df_wise = df_wise.toDF(*w_cols).select('w1mpro','w1sigmpro','w2mpro','w2sigmpro','w3mpro','w3sigmpro','w4mpro','w4sigmpro','oid')
    # 3) sgscore
    df_sgscore = spark.read.option("header", True).csv(input_sgscore).select("oid", "sgscore1")

    df_features = df_features.join(df_wise, on="oid", how="left")
    df_features = df_features.join(df_sgscore, on="oid", how="left")

    df_features = df_features \
        .withColumn("W1-W2", df_features["w1mpro"] - df_features["w2mpro"]) \
        .withColumn("W2-W3", df_features["w2mpro"] - df_features["w3mpro"]) \
        .withColumn("g-W2", df_features["mean_mag_1"] - df_features["w2mpro"]) \
        .withColumn("g-W3", df_features["mean_mag_1"] - df_features["w3mpro"]) \
        .withColumn("r-W2", df_features["mean_mag_2"] - df_features["w2mpro"]) \
        .withColumn("r-W3", df_features["mean_mag_2"] - df_features["w3mpro"])
    columns_to_drop = ['w1mpro', 'w1sigmpro', 'w2mpro', 'w2sigmpro', 'w3mpro', 'w3sigmpro', 'w4mpro', 'w4sigmpro']
    df_features = df_features.drop(*columns_to_drop)

    df_features.write.save(output_features)
    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))


if __name__ == "__main__":
    join_data()
