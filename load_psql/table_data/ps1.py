from .generic import TableData
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import min as spark_min
from .table_columns import ps1_col


class PS1TableData(TableData):
    def select(self, obj_cid_window, fun=min):
        # logging.info("Processing ps1")
        ps1_col.remove("objectId")
        ps1_col.remove("unique1")
        ps1_col.remove("unique2")
        ps1_col.remove("unique3")

        tt_ps1 = self.dataframe.select("objectId", *[col(c) for c in ps1_col])

        tt_ps1_min = (
            tt_ps1.withColumn(
                "mincandid", spark_min(col("candid")).over(obj_cid_window)
            )
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
