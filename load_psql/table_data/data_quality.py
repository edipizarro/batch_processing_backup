from .generic import TableData
from pyspark.sql.functions import col
from .table_columns import qua_col


class DataQualityTableData(TableData):
    def select(self, tt_det):
        # logging.info("Processing dataquality")
        qua_col.remove("objectId")
        qua_col.remove("candid")
        tt_qua = tt_det.select(
            "objectId", "candid", *[col("c." + c).alias(c) for c in qua_col]
        )
        return tt_qua
