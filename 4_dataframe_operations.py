from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min


def main():
    spark = SparkSession.builder.appName("DataFrame Operations").getOrCreate()
    
    data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    columns = ["language", "users_count"]
    df = spark.createDataFrame(data=data, schema=columns)

    print("Show:")    
    df.show()

    print("Columns:")
    for col in df.columns:
        print(col)
    
    
    print("Describe:")
    df.describe().show()
    
    print("Print Schema")
    df.printSchema()
    
    print("select ")
    df.select("language", "users_count").show()
    
    print("filter")
    df.filter(df["users_count"] == 20000).show()
    
    print("group by")
    df.groupBy("language").count().show()
    
    print("order by")
    df.orderBy("users_count").show()
    
    print("limit")
    df.limit(1).show()
    
    print("drop")
    new_df=df.drop("language")
    new_df.show()
    
    #Aggregations
    df.groupby("language").agg(
        count("users_count").alias("count"),
        avg("users_count").alias("avg"),
        max("users_count").alias("max"),
        min("users_count").alias("min")
    ).show()
    
    print("sorting")
    df.sort("users_count").show()
    
    #WithColumn
    df.withColumn(
        "new_column",
        df["users_count"]*2
        ).show()
    
    #withColumnRenamed
    df.withColumnRenamed(
        "users_count",
        "users_count_new"
        )
    


if __name__ == "__main__":
   main()