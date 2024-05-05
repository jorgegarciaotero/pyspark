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

#Rename market.cap
data=data.withColumnRenamed("market.cap","market_cap")

#Schema
data.schema
data.columns
 
#Add column
data = data.withColumn('date_new',data["date"]) #creates new column "date"
data.schema
#DropColumn
data = data.drop("date_new")

#Data Selection: Select, Filter, Between, When, Like, GroupBy, Aggregations
data.select(['open','close','high','market_cap']).describe().show()

data.filter(
    (col('date') >= lit('2020-01-01')) &
    (col('date') <= lit('2020-01-31'))
).show()


from pyspark.sql.functions import when
data.select('open', 'close', when(data.adjusted >= 200.0, 1).otherwise(0).alias('new_column')).show(5)
data.select(['industry','open','close','adjusted']).groupBy('industry').mean().show()

from pyspark.sql.functions import col,max,min,avg,lit
data.groupBy('sector').agg(
        min('date').alias('min_date'),
        max('date').alias('max_date'),
        min('open').alias('min_open'),
        max('open').alias('max_open'),
        avg('open').alias('avg_open')
    ).show(truncate=False)