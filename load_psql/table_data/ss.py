from .generic import TableData
from pyspark.sql.functions import col
from pyspark.sql.functions import min as spark_min
from pyspark.sql import Window
from pyspark.sql.types import LongType


class SSTableData(TableData):
    def select(self, column_list, tt_det, obj_cid_window):
        # logging.info("Processing ss")
        column_list.remove("objectId")
        column_list.remove("candid")
        tt_ss = tt_det.select(
            "objectId", "candid", *[col("c." + c).alias(c) for c in column_list]
        )
        tt_ss_min = (
            tt_ss.withColumn("mincandid", spark_min(col("candid")).over(obj_cid_window))
            .where(col("candid") == col("mincandid"))
            .select("objectId", tt_ss["candid"].cast(LongType()), *column_list)
        )
        return tt_ss_min
