from .generic import TableData
from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import min as spark_min


class ReferenceTableData(TableData):
    def select(self, column_list, tt_det):
        column_list.remove("objectId")
        column_list.remove("rfid")

        tt_ref = tt_det.select("rfid", "objectId", *[col(c) for c in column_list])
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
            .select("rfid", "objectId", *column_list)
        )

        return tt_ref_min
