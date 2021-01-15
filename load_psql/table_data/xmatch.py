from .generic import TableData
from pyspark.sql.functions import col, lit

#oid, catid, oid_catalog,dist

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
        )

        return sel_xmatch
