from .generic import TableData
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.types import IntegerType


class GaiaTableData(TableData):
    def compare_threshold(self, val, threshold):
        return val > threshold

    def select(self, column_list, tt_det, obj_cid_window, real_threshold=1e-4):
        column_list.remove("objectId")
        column_list.remove("candid")
        column_list.remove("unique1")

        tt_det = tt_det.where(col("has_stamp"))
        tt_gaia = tt_det.select(
            "objectId", "candid", *[col("c." + c).alias(c) for c in column_list]
        )

        tt_gaia_min = (
            tt_gaia.withColumn(
                "mincandid",
                spark_min(col("candid")).over(obj_cid_window),
            )
            .where(col("candid") == col("mincandid"))
            .select("objectId", "candid", *column_list)
        )

        data_gaia = (
            tt_gaia_min.alias("i")
            .join(tt_gaia.alias("c"), "objectId", "inner")
            .select(
                "objectId",
                "i.candid",
                col("i.maggaia").alias("min_maggaia"),
                *[col("i." + c).alias(c) for c in column_list]
            )
            .withColumn(
                "unique1",
                self.compare_threshold(
                    spark_abs(col("min_maggaia") - col("maggaia")),
                    real_threshold,
                ),
            )
            .drop("min_maggaia")
        )

        gr_gaia = (
            data_gaia.groupBy("objectId", "candid", *column_list)
            .agg(countDistinct("unique1").alias("count1"))
            .withColumn("unique1", col("count1") == 1)
            .drop("count1")
        )

        return gr_gaia
