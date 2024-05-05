from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min


def main():
    spark = SparkSession.builder.appName("DataFrame Operations").getOrCreate()

    # Create some sample data (replace with your actual data source)
    data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    columns = ["language", "users_count"]
    df = spark.createDataFrame(data=data, schema=columns)

    # Register DataFrame as a temporary view
    df.createOrReplaceTempView("temptable")

    # Use SQL to query the temporary view
    spark.sql("select * from temptable").show()


    #HIVE:
    #df = spark.read.format("hive").load("hive://default.mytable")
    #OR
    #spark.sql("CREATE TEMPORARY VIEW mytable_view AS SELECT * FROM hive://default.mytable")
    #df = spark.sql("SELECT * FROM mytable_view")


if __name__ == "__main__":
    main()
    