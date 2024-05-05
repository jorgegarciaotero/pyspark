from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configs spark session
spark = SparkSession.builder \
    .appName('Test Spark Script') \
    .getOrCreate() #Returns the existing spark session or creates a new one

# Creates a Spark Dataframe from a list of rndm numbers
data = [(i,) for i in range(1, 101)]
df = spark.createDataFrame(data, ['number'])

#Calculates the sum of the numbers
total_sum = df.select(col('number')).groupBy().sum().collect()[0][0]

print("The sum is:", total_sum)

# Closes the session
spark.stop()
