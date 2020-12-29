from .generic import TableData
from pyspark.sql.functions import col
from load_psql.table_data.table_columns import non_col


class NonDetectionTableData(TableData):
    def select(self):
        # logging.info("Processing non detections")
        tt_non = self.dataframe.withColumn("mjd", col("jd") - 2400000.5).drop("jd")
        sel_non = tt_non.select(*[col(c) for c in non_col])
        return sel_non

    def save(self, n_partitions, max_records_per_file, mode, output_dir, selected=None):
        sel_non = selected or self.dataframe
        sel_non.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir + "non_detection", emptyValue="")
