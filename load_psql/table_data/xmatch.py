from .generic import TableData
from .table_columns import xmatch_col
from pyspark.sql.functions import col


class XmatchTableData(TableData):
    def select(self):
        xmatch_col.remove("catid")

        sel_xmatch = self.dataframe.select(
            *[col(c) for c in xmatch_col],
        ).withColumnRenamed("designation","oid_catalog")\
        .withColumnRenamed("objectId_2","oid")\
        .withColumnRenamed("distance", "dist")\
        .withColumn("step_id_corr", lit("allwise"))

        return sel_allwise

    def save(self, output_dir, n_partitions, max_records_per_file, mode, selected=None):
        sel_xmatch = selected or self.dataframe
        sel_xmatch.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir, emptyValue="")
