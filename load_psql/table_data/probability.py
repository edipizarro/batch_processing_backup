from load_psql.table_data import TableData
from pyspark.sql import DataFrame, DataFrameReader, SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import split, explode, struct, lit, col, array, when, substring, dense_rank, desc


class ProbabilityTableData(TableData):
    def select(self, column_list: list, tt_det: DataFrame, step_id: str) -> DataFrame:
        data_prob = (


df = spark.read.load("/volume2/lsabatini/abult", format="csv", inferSchema="true", header="true")  

prob_col = ['AGN_prob_org', 'Blazar_prob_org', 'CV/Nova_prob_org', 'QSO_prob_org', 'YSO_prob_org', 'SLSN_prob_org', 'SNII_prob_org', 'SNIa_prob_org', 'SNIbc_prob_org', 'CEP_prob_org', 'DSCT_prob_org', 'E_prob_org', 'LPV_prob_org', 'Periodic-Other_prob_org', 'RRL_prob_org', 'AGN_prob', 'Blazar_prob', 'CV/Nova_prob', 'QSO_prob', 'YSO_prob', 'SLSN_prob', 'SNII_prob', 'SNIa_prob', 'SNIbc_prob', 'CEP_prob', 'DSCT_prob', 'E_prob', 'LPV_prob', 'Periodic-Other_prob', 'RRL_prob', 'prob_Periodic', 'prob_Stochastic', 'prob_Transient', 'oid']

df = df_prob.select(*[col(c) for c in prob_col]).withColumnRenamed("prob_Periodic", "Periodic_prob_top").withColumnRenamed("prob_Stochastic", "Stochastic_prob_top").withColumnRenamed("prob_Transient", "Transient_prob_top").withColumnRenamed("AGN_prob", "AGN_prob_S").withColumnRenamed("Blazar_prob", "Blazar_prob_S").withColumnRenamed("CV/Nova_prob", "CV/Nova_prob_S").withColumnRenamed("QSO_prob", "QSO_prob_S").withColumnRenamed("YSO_prob", "YSO_prob_S").withColumnRenamed("SLSN_prob", "SLSN_prob_T").withColumnRenamed("SNII_prob", "SNII_prob_T").withColumnRenamed("SNIa_prob", "SNIa_prob_T").withColumnRenamed("SNIbc_prob", "SNIbc_prob_T").withColumnRenamed("CEP_prob", "CEP_prob_P").withColumnRenamed("DSCT_prob", "DSCT_prob_P").withColumnRenamed("E_prob", "E_prob_P").withColumnRenamed("LPV_prob", "LPV_prob_P").withColumnRenamed("Periodic-Other_prob", "Periodic-Other_prob_P").withColumnRenamed("RRL_prob", "RRL_prob_P")

cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in ["oid"])) 

kvs = explode(array([
      struct(lit(c).alias("key"), col(c).alias("value")) for c in cols
    ])).alias("kvs")

df2 = df.select(["oid"] + [kvs]).select(["oid"] + ["kvs.key", "kvs.value"]).withColumn("classifier_version", lit("bulk_0.0.2")).withColumn("class_name", (split(col("key"), "_").getItem(0))).withColumn("probability",(col("value")))

df4 = df2.withColumn("classifier_name", when((substring(col("key"), -3, 3) == "top"), "lc_classifier_top")
                                       .when((substring(col("key"), -1, 1) == "T"), "lc_classifier_transient")
                                       .when((substring(col("key"), -1, 1) == "S"), "lc_classifier_stochastic")
                                       .when((substring(col("key"), -1, 1) == "P"), "lc_classifier_periodic")
                                       .when((substring(col("key"), -3, 3) == "org"), "lc_classifier")
                                       .otherwise("NaN")).select(df2.probability.cast(DoubleType()),"oid","classifier_name","classifier_version","class_name","value","key")


df5 = df4.withColumn("ranking", dense_rank().over(Window.partitionBy("oid","classifier_name").orderBy(desc("probability"))))

df6 = df5.drop("key").drop("value")

df7 = df6.select("oid","classifier_name","classifier_version","class_name","probability","ranking")

sel = df7

sel.repartition(10).write.option("maxRecordsPerFile", 100000).mode("overwrite").csv("/volume2/lsabatini/probabilities",emptyValue='')


