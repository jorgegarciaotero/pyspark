import pandas as pd
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


#Create dataframe manually
data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
columns = ["language", "users_count"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show()


#Create a dataframe from a  Hive table.
#df_hive = spark.sql("select * from hive_table")
#df_hive.show()

#CSV
df=spark.read.csv(r"C:\proyectos\spark\data\people.csv", header=True, inferSchema=True,sep="|")
df.printSchema()
df.show()
df.write.csv(r"C:\proyectos\spark\data\people.csv", header=True, mode="overwrite")

#Parquet
df=spark.read.parquet(r"C:\proyectos\spark\data\people.parquet")



