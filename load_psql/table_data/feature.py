from load_psql.table_data import TableData
from pyspark.sql import DataFrame, DataFrameReader, SparkSession, Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import split, explode, struct, lit, col, array, when, substring, dense_rank, desc, expr

class FeatureTableData(TableData):
    def select(self, column_list: list, df: DataFrame, step_id: str) -> DataFrame:
        
		cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in ["oid"])) 
		
		kvs = explode(array([
		      struct(lit(c).alias("key"), col(c).alias("value")) for c in cols
		    ])).alias("kvs")
		
		df2 = df.select(["oid"] + [kvs]).select(["oid"] + ["kvs.key", "kvs.value"]).withColumn("fid",
		                                                                                           when((col("key") == "gal_b") | (col("key") == "gal_l") | (col("key") == "rb") | (col("key") == "sgscore1") | (col("key") == "W1") | (col("key") == "W1-W2") | (col("key") == "W2") | (col("key") == "W2-W3") | (col("key") == "W3") | (col("key") == "W4") | (col("key") == "g-W2") | (col("key") == "g-W3") | (col("key") == "g-r_ml") | (col("key") == "r-W2") | (col("key") == "r-W3"), 0)
		                                                                                          .when((col("key") == "g-r_max") | (col("key") == "g-r_max_corr") | (col("key") == "g-r_mean") | (col("key") == "g-r_mean_corr") | (col("key") == "Multiband_period") | (col("key") == "PPE"), 12)
		                                                                                          .otherwise(substring("key", -1, 1))).withColumn("name", when((col("fid") == 0) | (col("fid") == 12), (col("key"))).otherwise(expr("substring(key, 1, length(key)-2)"))).withColumn("version", lit("bulk_1.0.1"))
		df3 = df2.drop("key") 
		
		#df3.where(col('value').like("%E%")).show()
		
		df4 = df3.withColumn("value", when((col("value").like("%E%")), 0).otherwise(col("value")))
		
		sel_det = df4.select(*[col(c) for c in column_list])
		
        return sel_det