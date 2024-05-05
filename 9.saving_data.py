import pyspark.sql.functions as F  # Import alias for functions
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.appName('load_data').getOrCreate()


# Define data schema with corrected 'market.cap' type
data_schema = [
    StructField("_c0", IntegerType(), True),
    StructField("symbol", StringType(), True),
    StructField("date", DateType(), True),  # Renamed from "data" for clarity
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("adjusted", DoubleType(), True),
    StructField("market_cap", StringType(), True),  # Corrected type (remove dot)
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("exchange", StringType(), True)
]

final_struc = StructType(fields=data_schema)

# Read CSV with specified options
data = spark.read.csv("/opt/bitnami/spark/data/stocks_price_final.csv",
                       sep=",",
                       header=True,
                       schema=final_struc)

data.printSchema()
data.show()


data.write.csv("dataset.csv")
data.select(['symbol', 'date', 'market_cap']).write.csv("dataset_select.csv")

data.write.json("dataset.json")
data.write.parquet("dataset.parquet")
data.write.orc("dataset.orc")
data.write.text("dataset.txt")
