from .generic import TableData
from .table_columns import ref_col
from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import min as spark_min


class ReferenceTableData(TableData):
    def select(self, tt_det):
        ref_col.remove("objectId")
        ref_col.remove("rfid")

        tt_ref = tt_det.select("rfid", "objectId", *[col(c) for c in ref_col])
        obj_rfid_cid_window = Window.partitionBy("objectId", "rfid").orderBy("candid")

        tt_ref_min = (
            tt_ref.withColumn(
                "auxcandid",
                spark_min(col("candid")).over(obj_rfid_cid_window),
            )
            .withColumn("jdstartref", tt_ref.jdstartref - 2400000.5)
            .withColumn("jdendref", tt_ref.jdendref - 2400000.5)
            .withColumnRenamed("jdstartref", "mjdstartref")
            .withColumnRenamed("jdendref", "mjdendref")
            .where(col("candid") == col("auxcandid"))
            .select("rfid", "objectId", *ref_col)
        )

        return tt_ref_min
