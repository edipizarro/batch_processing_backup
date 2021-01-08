from .generic import TableData
from pyspark.sql.functions import col
from pyspark.sql.functions import min as spark_min
from pyspark.sql import Window
from .table_columns import ss_col


class SSTableData(TableData):
    def select(self, tt_det, obj_cid_window):
        # logging.info("Processing ss")
        ss_col.remove("objectId")
        ss_col.remove("candid")
        tt_ss = tt_det.select(
            "objectId", "candid", *[col("c." + c).alias(c) for c in ss_col]
        )
        tt_ss_min = (
            tt_ss.withColumn("mincandid", spark_min(col("candid")).over(obj_cid_window))
            .where(col("candid") == col("mincandid"))
            .select("objectId", "candid", *ss_col)
        )
        return tt_ss_min
