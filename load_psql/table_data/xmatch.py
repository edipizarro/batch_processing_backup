from .generic import TableData
from pyspark.sql.functions import col, lit


class XmatchTableData(TableData):
    def select(self, column_list: list):
        column_list.remove("catid")

        sel_xmatch = (
            self.dataframe.select(
                *[col(c) for c in column_list],
            )
            .withColumnRenamed("designation", "oid_catalog")
            .withColumnRenamed("objectId_2", "oid")
            .withColumnRenamed("distance", "dist")
            .withColumn("step_id_corr", lit("allwise"))
        )

        return sel_xmatch
