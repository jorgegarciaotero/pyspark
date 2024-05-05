'''
Actions: Operations over RDD that executes inmediately and return a value.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configs spark session
spark = SparkSession.builder \
    .appName('Test Spark Script') \
    .getOrCreate() #Returns the existing spark session or creates a new one
sc = spark.sparkContext  #retrieves the SparkContext from the created SparkSession. The SparkContext is the lower-level API that provides functionalities for distributed computing tasks like managing workers and executors in the cluster.


#count
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
print(rdd.count())

#first
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
print(rdd.first())

#take
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
print(rdd.take(3))

#collect
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
print(rdd.collect())

#reduce
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

