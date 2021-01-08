from .generic import TableData
from pyspark.sql.functions import col, lit
from .table_columns import mag_col


class MagstatsTableData(TableData):
    def select(self):
        # logging.info("Processing magstats")
        data_mag = self.dataframe.withColumn("magsigma", lit("")).withColumn(
            "magsigma_corr", lit("")
        )
        sel_mag = data_mag.select(*[col(c) for c in mag_col])
        return sel_mag
