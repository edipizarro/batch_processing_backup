from .generic import TableData
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


class DataQualityTableData(TableData):
    def select(self, column_list, tt_det):
        # logging.info("Processing dataquality")
        column_list.remove("objectId")
        column_list.remove("candid")
        tt_qua = tt_det.select(
            "objectId",
            tt_det["candid"].cast(IntegerType()),
            *[col("c." + c).alias(c) for c in column_list]
        )
        return tt_qua
