from load_psql.table_data import TableData
from pyspark.sql import DataFrame, DataFrameReader, SparkSession, Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import (
    split,
    explode,
    struct,
    lit,
    col,
    array,
    when,
    substring,
    dense_rank,
    desc,
    expr,
)


class FeatureTableData(TableData):
    def select(self, column_list, version):
        cols, dtypes = zip(
            *((c, t) for (c, t) in self.dataframe.dtypes if c not in ["oid"])
        )
        kvs = explode(
            array([struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])
        ).alias("kvs")
        when_expr = when(
            (col("key") == "gal_b")
            | (col("key") == "gal_l")
            | (col("key") == "rb")
            | (col("key") == "sgscore1")
            | (col("key") == "W1")
            | (col("key") == "W2")
            | (col("key") == "W3")
            | (col("key") == "W4")
            | (col("key") == "W1-W2")
            | (col("key") == "W2-W3")
            | (col("key") == "r-W3")
            | (col("key") == "r-W2")
            | (col("key") == "g-W3")
            | (col("key") == "g-W2")
            | (col("key") == "g-r_ml"),
            0,
        ).when(
            (col("key") == "g-r_max")
            | (col("key") == "g-r_max_corr")
            | (col("key") == "g-r_mean")
            | (col("key") == "g-r_mean_corr")
            | (col("key") == "Multiband_period")
            | (col("key") == "PPE")
            | (col("key").startswith("Power_rate")),
            12,
        )
        df_fea = (
            self.dataframe.select(["oid"] + [kvs])
            .select(["oid"] + ["kvs.key", "kvs.value"])
            .withColumn("fid", when_expr.otherwise(substring("key", -1, 1)))
            .withColumn(
                "name",
                when((col("fid") == 0) | (col("fid") == 12), col("key")).otherwise(
                    expr("substring(key, 1, length(key)-2)")
                ),
            )
            .withColumn("version", lit(version))
            .drop("key")
            .withColumn(
                "value", when((col("value").like("%E%")), 0).otherwise(col("value"))
            )
        )

        sel_fea = df_fea.select(*column_list)
