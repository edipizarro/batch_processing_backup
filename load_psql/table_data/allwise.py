from .generic import TableData
from pyspark.sql.functions import col


class AllwiseTableData(TableData):
    def select(self, column_list: list):
        sel_allwise = self.dataframe.select(
            *[col(c) for c in column_list],
        ).withColumnRenamed("designation", "oid_catalog")

        return sel_allwise
