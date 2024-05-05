import pandas as pd
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('load_data').getOrCreate()
b_data= spark.read.csv("data/stocks_file_final.csv",
                       sep=",",
                       header=True,
                       inferSchema=True)
b_data.show()
b_data.printSchema()