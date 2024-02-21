from .generic import TableData
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

class XmatchTableData(TableData):
    def select(self, column_list: list):
        column_list.remove("objectId_2")
        column_list.remove("catid")

        sel_xmatch = (
            self.dataframe.select(
                col("objectId_2"),
                lit("allwise").alias("catid"),
                *[col(c) for c in column_list],
            )
            .withColumnRenamed("designation", "oid_catalog")
            .withColumnRenamed("objectId_2", "oid")
            .withColumnRenamed("distance", "dist")
            .withColumn("class_catalog", lit(None).cast(StringType()))
            .withColumn("period", lit(None).cast(StringType()))
            .dropDuplicates(["oid", "catid"])
        )

        return sel_xmatch
