from .generic import TableData
from pyspark.sql.functions import col
from pyspark.sql import Window
from .table_columns import ss_col


class SSTableData(TableData):
    def get_min(self, name):
        return min(col(name))

    def select(self, tt_det):
        # logging.info("Processing ss")
        ss_col.remove("objectId")
        ss_col.remove("candid")
        tt_ss = tt_det.select(
            "objectId", "candid", *[col("c." + c).alias(c) for c in ss_col]
        )
        obj_cid_window = Window.partitionBy("objectId").orderBy("candid")
        tt_ss_min = (
            tt_ss.withColumn("mincandid", self.get_min("candid").over(obj_cid_window))
            .where(col("candid") == col("mincandid"))
            .select("objectId", "candid", *ss_col)
        )
        return tt_ss_min, obj_cid_window

    def save(self, output_dir, n_partitions, max_records_per_file, mode, selected):
        # logging.info("Writing ss")
        tt_ss_min = selected or self.dataframe
        tt_ss_min.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir + "ss_ztf", emptyValue="")
