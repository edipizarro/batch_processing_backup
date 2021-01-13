from .generic import TableData
from .table_columns import allwise_col
from pyspark.sql.functions import col


class AllwiseTableData(TableData):
    def select(self):
        sel_allwise = self.dataframe.select(
            *[col(c) for c in allwise_col],
        ).withColumnRenamed("designation","oid_catalog")

        return sel_allwise

    def save(self, output_dir, n_partitions, max_records_per_file, mode, selected=None):
        sel_allwise = selected or self.dataframe
        sel_allwise.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir, emptyValue="")
