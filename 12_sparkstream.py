from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("structurednetworkcount").getOrCreate()

#Create DF representing the steram of input lines from connection to localhost:9999
lines= spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

#split the lines into words
words  = lines.select(explode(split(lines.value," ")).alias("word"))

#Generate running word count
wordCounts = words.groupBy("word").count()

#Start running the query that prints the running counts to the console
'''
Spark uses various modes of output to store the data:
Complete: all the table will be stored.
append: only the new rows will be stored. 
update: only the updated rows will be stored.
'''
query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()

