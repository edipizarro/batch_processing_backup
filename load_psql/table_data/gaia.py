from .generic import TableData
from .table_columns import gaia_col
from pyspark.sql import Window
from pyspark.sql.functions import col, countDistinct


class GaiaTableData(TableData):
    def select(self, tt_det, obj_cid_window, real_threshold=1e-4):
        gaia_col.remove("objectId")
        gaia_col.remove("candid")
        gaia_col.remove("unique1")

        tt_gaia = tt_det.select('objectId', 'candid', *[col('c.' + c).alias(c) for c in gaia_col])

        tt_gaia_min = tt_gaia.withColumn('mincandid', min(col('candid')).over(obj_cid_window)) \
            .where(col('candid') == col('mincandid')).select('objectId', 'candid', *gaia_col)

        data_gaia = tt_gaia_min.alias('i').join(tt_gaia.alias('c'), 'objectId', 'inner').select('objectId', 'i.candid',
                                                                                                col('i.maggaia').alias(
                                                                                                    'min_maggaia'),
                                                                                                *[col('i.' + c).alias(c)
                                                                                                  for c in
                                                                                                  gaia_col]).withColumn(
            'unique1', abs(col('min_maggaia') - col('maggaia')) > real_threshold).drop('min_maggaia')

        gr_gaia = data_gaia.groupBy('objectId', 'candid', *gaia_col).agg(
            countDistinct('unique1').alias('count1')).withColumn("unique1", col('count1') != 1).drop('count1')

        return gr_gaia

    def save(self, output_dir, n_partitions, max_records_per_file, mode, selected=None):
        sel_ref = selected or self.dataframe
        sel_ref.coalesce(n_partitions).write.option(
            "maxRecordsPerFile", max_records_per_file
        ).mode(mode).csv(output_dir + "reference", emptyValue="")
