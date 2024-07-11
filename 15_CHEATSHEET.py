# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("cheat_sheet").getOrCreate()
cores=spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print("you are working with ",cores, " cores")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create empty DataFrame

# COMMAND ----------

values=[('Pear',10),('Orange',13),('Peach',5)]
df=spark.createDataFrame(values,['fruit','quant'])
df=df.withColumnRenamed('quant','amount')
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read from files

# COMMAND ----------

#Read from CSV
dataframe_csv = spark.read.csv("/FileStore/tables/students.csv", inferSchema=True,header=True)
dataframe_csv.show(20)
dataframe_csv.limit(10).toPandas() #much more visual

# COMMAND ----------

# Read 1 file from Parquet
dataframe_parquet = spark.read.parquet('/FileStore/tables/users1.parquet')
dataframe_parquet.limit(10).toPandas()

# COMMAND ----------

#Read multiple Parquet Files
dataframe_parquet_partitioned = spark.read.parquet('/FileStore/tables/users*')
dataframe_parquet_partitioned.limit(10).toPandas()

# COMMAND ----------

dataframe_users1_2=spark.read.parquet('/FileStore/tables/users1.parquet','/FileStore/tables/users2.parquet')
dataframe_users1_2.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataframe information

# COMMAND ----------

dataframe_csv.describe() #column name and types
dataframe_csv.columns    #column names
 

# COMMAND ----------

dataframe_csv.count() #num of rows


# COMMAND ----------

dataframe_csv.select('math score','reading score').summary('count','min','max').show()

# COMMAND ----------

data_schema=[StructField('name',StringType(),True),
             StructField('email',StringType(),True),
             StructField('city',StringType(),True),
             StructField('mac',StringType(),True),
             StructField('timestamp',DateType(),True),
             StructField('credicard',StringType(),True)]
final_struct=StructType(fields=data_schema)
dataframe_people=spark.read.json('/FileStore/tables/people.json',schema=final_struct)
dataframe_people.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save files

# COMMAND ----------

dataframe_people.write.mode('overwrite').csv('write_test.csv')
dataframe_people.write.mode('overwrite').parquet('parquet/')
dataframe_users1_2.write.mode('overwrite').partitionBy('gender').parquet('part_parquet/') #partition by gender male/female

# COMMAND ----------

# MAGIC %md
# MAGIC #### SPARK "ORM"

# COMMAND ----------

fifa=spark.read.csv('/FileStore/tables/fifa19.csv',inferSchema=True,header=True)
fifa.limit(4).toPandas()

# COMMAND ----------

from pyspark.sql.functions import *
fifa.select(['Nationality','Name','Age','Photo']).show(10,True) # See the whole string line with show=False. "..." if true

# COMMAND ----------

fifa.select(['Name','Age']).orderBy(fifa['Age']).show(5)

# COMMAND ----------

fifa.select(['Name','Age']).orderBy(fifa['Age'].desc()).show(10)

# COMMAND ----------

fifa.select(['Name','Club']).where(fifa.Club.like('%Barcelona%')).show()

# COMMAND ----------

fifa.select('Name','Club').where(fifa.Name.startswith("L")).where(fifa.Name.endswith('i')).show(5)

# COMMAND ----------

fifa[fifa.Club.isin('FC Barcelona','Juventus')].limit(5).toPandas()

# COMMAND ----------

fifa.filter('age=31').select(['Name','Age']).limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SPARKSQL

# COMMAND ----------

fifa.createOrReplaceTempView("tempview")
spark.sql("SELECT ID,Name,Age, Photo,Nationality FROM tempview WHERE Age>25").limit(5).toPandas()

# COMMAND ----------

spark.sql("SELECT Nationality,Count(ID) as Total FROM tempview GROUP BY Nationality").limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRANSFORMATIONS

# COMMAND ----------

videos=spark.read.csv('/FileStore/tables/youtubevideos.csv',header=True,inferSchema=True)
videos.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
#Replace columns by correct types
df = videos\
  .withColumn('views',videos['views'].cast(IntegerType())) \
  .withColumn('likes',videos['likes'].cast(IntegerType())) \
  .withColumn('dislikes',videos['dislikes'].cast(IntegerType())) \
  .withColumn('trending_date',to_date(videos.trending_date,'yy.dd.mm')) \
  #.withColumn('publish_time',to_timestamp(videos.publish_time,'yyyy-MM-dd HH:mm:dd')) 
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
df=df.withColumn('publish_time_2',regexp_replace(df.publish_time,'T',' '))
df=df.withColumn('publish_time_2',regexp_replace(df.publish_time_2,'Z',''))
df=df.withColumn('publish_time_3',to_timestamp(df.publish_time_2,'yyy-MM-dd HH:mm:ss.SSS'))
df.select('publish_time','publish_time_2','publish_time_3').show(5,False)

# COMMAND ----------

#Lower
df.select('title',lower(df.title).alias("lower_title")).show(5,False)

# COMMAND ----------

#Case when
#option1: when-otherwise
df.select('likes','dislikes',when(df.likes>df.dislikes,'Good').when(df.likes<df.dislikes,'Bad').otherwise('Undetermined').alias('Favorability')).show(5)

# COMMAND ----------

#Case when
#option2: expr
df.select('likes','dislikes',expr("CASE WHEN likes > dislikes THEN 'Good' WHEN dislikes > likes THEN 'BAD' ELSE 'UNDT' END AS Favorability")).show(5)

# COMMAND ----------

#Concatenate
df.select(concat_ws(' ',df.title,df.channel_title).alias('text')).show(5,False)

# COMMAND ----------

#Extract year, month from date
df.select('trending_date',year('trending_date'),month('trending_date')).show(5)

# COMMAND ----------

#datediff: 
df.select('trending_date','publish_time_3',datediff(df.publish_time_3,df.trending_date ).alias('datediff')).show(5)

# COMMAND ----------

#split array titles by ' '
array=df.select('title',split(df.title,' ').alias('new'))
array.show(5,False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####GROUP BY

# COMMAND ----------

airbnb=spark.read.csv("/FileStore/tables/nyc_air_bnb.csv",inferSchema=True,header=True)
df = airbnb.withColumn('price',airbnb['price'].cast(IntegerType())) \
  .withColumn('minimum_nights',airbnb['minimum_nights'].cast(IntegerType())) \
  .withColumn('number_of_reviews',airbnb['number_of_reviews'].cast(IntegerType())) \
  .withColumn('reviews_per_month',airbnb['reviews_per_month'].cast(IntegerType())) \
  .withColumn('calculated_host_listings_count',airbnb['calculated_host_listings_count'].cast(IntegerType())) 
df.printSchema()

# COMMAND ----------

df.groupBy("neighbourhood_group").count().show(7)

# COMMAND ----------

df.groupBy("neighbourhood").agg({'price':'mean'}).show(7)

# COMMAND ----------

df.groupBy("neighbourhood").agg(min(df.price).alias('min_price'),max(df.price).alias('max_price')).show(7)

# COMMAND ----------

summary=df.summary('count','min','max',"25%","75%","max")
summary.limit(5).toPandas()

# COMMAND ----------

df.select(countDistinct('neighbourhood_group'),avg('price'),stddev('price')).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGES
# MAGIC

# COMMAND ----------



# COMMAND ----------

valuesP=[('koala',1,'yes'),('caterpillar',2,'yes'),('deer',3,'yes'),('human',4,'yes')]
eats_plants=spark.createDataFrame(valuesP,['name','id','eats_plants'])

valuesM=[('shark',5,'no'),('lion',6,'no'),('tiger',7,'no'),('human',4,'no')]
eats_meat=spark.createDataFrame(valuesM,['name','id','eats_plants'])

print("Plant eaters (herbivores)")
print(eats_plants.show())

print("Meat eaters (carnivores)")
print(eats_meat.show())

# COMMAND ----------

new_df=eats_plants
df_append=eats_plants.union(new_df)

# COMMAND ----------

#inner join
inner_join=eats_plants.join(eats_meat,['name','id'],'inner')
inner_join.show()

# COMMAND ----------

#full join
full_join=eats_plants.join(eats_meat,['name','id'],'full')
full_join.show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
