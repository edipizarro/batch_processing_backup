from .generic import TableData
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col


class ObjectTableData(TableData):
    def select(self, column_list):
        sel_obj = self.dataframe.select(
            *[
                col(c).cast(IntegerType()) if c in ["ndethist", "ncovhist"] else col(c)
                for c in column_list
            ],
        )
        return sel_obj
