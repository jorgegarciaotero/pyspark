from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("SparkStreaming").getOrCreate()


jsonSchema = StructType([
  StructField("name", StringType(), True),
  StructField("age", IntegerType(), True),
  # Add more fields as needed based on your JSON structure
])

streamingInputDF = (spark
    .readStream
    .schema(jsonSchema) #set a schema of the json data
    .option("maxFilesPerTrigger", 1) #treat a sequence of files as a stream by picking one file at a time 
    .json("data/")
)

streamingCountsDF = (
    streamingInputDF.groupBy(
        streamingInputDF.action,
        window(streamingInputDF.timestamp, "1 hour")
    ).count()
)

streamingCountsDF.isStreaming