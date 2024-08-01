from pyspark.sql import SparkSession
import random

# Initialize Spark session
spark = SparkSession.builder.appName("RandomDataFrame").getOrCreate()

# Create a schema for the DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", FloatType(), True)
])

# Generate random data
data = [
    (i, f'name_{i}', random.randint(20, 60), round(random.uniform(30000, 100000), 2)) for i in range(1, 11)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
print("SHOWED")

from batch_processing.pipeline import correction
print("IMPORTED")