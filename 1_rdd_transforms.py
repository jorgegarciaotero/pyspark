'''
Transforms are lazy, they don't execute until an action is called.
- map: Maps the elements of a dataset to another dataset specified by a function.
- filter:
- flatMap:
- distinct: 
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configs spark session
spark = SparkSession.builder \
    .appName('Test Spark Script') \
    .getOrCreate() #Returns the existing spark session or creates a new one
sc = spark.sparkContext  #retrieves the SparkContext from the created SparkSession. The SparkContext is the lower-level API that provides functionalities for distributed computing tasks like managing workers and executors in the cluster.

#Creates an rdd:
num = [1,2,3,4,5]
num_rdd = sc.parallelize(num)
print("num_rdd: ", num_rdd)
collected_data = num_rdd.collect()
print("Collected Data:", collected_data)

#Map:
double_rdd=num_rdd.map(lambda x: x*2)
print("Double RDD:", double_rdd.collect())

#Filter:
even_rdd=num_rdd.filter(lambda x: x%2==0)
print("Even RDD:", even_rdd.collect())

#Distinct:
distinct_rdd=num_rdd.distinct()
print("Distinct RDD:", distinct_rdd.collect())  

#FlatMap:
flat_rdd=num_rdd.flatMap(lambda x: [x,x+1])
print("Flat RDD:", flat_rdd.collect())

#reduceByKey: 
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
print("RDD:", rdd.collect())
rdd_reduced = rdd.reduceByKey(lambda x, y: x + y)
print("RDD reduced:", rdd_reduced.collect())

#groupBykey:
rdd_grouped = rdd.groupByKey()
print("RDD grouped:", rdd_grouped.collect())

#sortByKey:
rdd_sorted = rdd.sortByKey()
print("RDD sorted:", rdd_sorted.collect())

#join:
rdd_join = rdd.join(rdd_sorted)
print("RDD join:", rdd_join.collect())

# Closes the session
spark.stop()
