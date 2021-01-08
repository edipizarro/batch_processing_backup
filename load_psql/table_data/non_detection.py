from .generic import TableData
from pyspark.sql.functions import col
from load_psql.table_data.table_columns import non_col


class NonDetectionTableData(TableData):
    def select(self):
        # logging.info("Processing non detections")
        tt_non = self.dataframe.withColumn("mjd", col("jd") - 2400000.5).drop("jd")
        sel_non = tt_non.select(*[col(c) for c in non_col])
        return sel_non
