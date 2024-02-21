from .generic import TableData
from pyspark.sql.functions import col, lit


class MagstatsTableData(TableData):
    def select(self, column_list):
        # logging.info("Processing magstats")
        data_mag = self.dataframe.withColumn("magsigma", lit("")).withColumn(
            "magsigma_corr", lit("")
        )
        sel_mag = data_mag.select(*[col(c) for c in column_list])
        return sel_mag
