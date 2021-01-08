from .generic import TableData
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col


class ObjectTableData(TableData):
    def select(self, column_list):
        column_list.remove("objectId")
        column_list.remove("ndethist")
        column_list.remove("ncovhist")

        sel_obj = self.dataframe.select(
            "objectId",
            self.dataframe.ndethist.cast(IntegerType()),
            self.dataframe.ncovhist.cast(IntegerType()),
            *[col(c) for c in column_list],
        )
        return sel_obj
