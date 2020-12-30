from .generic import TableData
from .table_columns import ref_col
from pyspark.sql import Window
from pyspark.sql.functions import col


class ReferenceTableData(TableData):
    def apply_fun(self, fun, column):
        return fun(column)

    def select(self, tt_det, fun=min):
        ref_col.remove("objectId")
        ref_col.remove("rfid")

        tt_ref = tt_det.select("rfid", "objectId", *[col(c) for c in ref_col])
        obj_rfid_cid_window = Window.partitionBy("objectId", "rfid").orderBy("candid")

        tt_ref_min = (
            tt_ref.withColumn(
                "auxcandid",
                self.apply_fun(fun, col("candid")).over(obj_rfid_cid_window),
            )
            .where(col("candid") == col("auxcandid"))
            .select("rfid", "objectId", *ref_col)
        )

        return tt_ref_min

    def save(self, output_dir, n_partitions, max_records_per_file, mode, selected=None):
        sel_ref = selected or self.dataframe
        sel_ref.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir + "reference", emptyValue="")
