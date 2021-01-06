from .generic import TableData
from .table_columns import obj_col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col


class ObjectTableData(TableData):
    def select(self):
        obj_col.remove("objectId")
        obj_col.remove("ndethist")
        obj_col.remove("ncovhist")

        sel_obj = self.dataframe.select(
            "objectId",
            self.dataframe.ndethist.cast(IntegerType()),
            self.dataframe.ncovhist.cast(IntegerType()),
            *[col(c) for c in obj_col],
        )
        return sel_obj

    def save(self, output_dir, n_partitions, max_records_per_file, mode, selected=None):
        sel_obj = selected or self.dataframe
        sel_obj.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir, emptyValue="")
