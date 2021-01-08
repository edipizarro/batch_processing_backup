from .generic import TableData
from pyspark.sql.functions import col


class NonDetectionTableData(TableData):
    def select(self, column_list):
        # logging.info("Processing non detections")
        column_list = column_list.copy()
        tt_non = self.dataframe.withColumn("mjd", col("jd") - 2400000.5).drop("jd")
        sel_non = tt_non.select(*[col(c) for c in column_list])
        return sel_non
