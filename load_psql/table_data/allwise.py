from .generic import TableData
from .table_columns import allwise_col
from pyspark.sql.functions import col


class AllwiseTableData(TableData):
    def select(self):
        sel_allwise = self.dataframe.select(
            *[col(c) for c in allwise_col],
        ).withColumnRenamed("designation","oid_catalog")

        return sel_allwise
