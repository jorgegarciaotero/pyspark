from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min


def main():
    spark = SparkSession.builder.appName("DataFrame Operations").getOrCreate()


    data1 = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    columns1 = ["name", "id"]

    df1 = spark.createDataFrame(data=data1, schema=columns1)

    data2 = [("Alice", 25), ("Charlie", 30), ("David", 40)]
    columns2 = ["name", "age"]

    df2 = spark.createDataFrame(data=data2, schema=columns2)

    print("DataFrame 1:")
    df1.show()

    print("DataFrame 2:")
    df2.show()


    #Inner Join
    df_inner_join = df1.join(df2, df1.id == df2.age, "inner")
    print("Inner Join:")
    df_inner_join.show()


    #Left Outer Join
    df_left_outer_join = df1.join(df2, df1.id == df2.age, "left_outer")
    print("Left Outer Join:")
    df_left_outer_join.show()

    #Right Outer Join
    df_right_outer_join = df1.join(df2, df1.id == df2.age, "right_outer")
    print("Right Outer Join:")
    df_right_outer_join.show()


    #Full Outer Join+
    df_full_outer_join = df1.join(df2, df1.id == df2.age, "full_outer")
    print("Full Outer Join:")
    df_full_outer_join.show()


    #Left Anti
    df_left_anti_join = df1.join(df2, df1.id == df2.age, "left_anti")
    print("Left Anti Join:")
    df_left_anti_join.show()


    #Cross Join
    df_cross_join = df1.crossJoin(df2)
    print("Cross Join:")
    df_cross_join.show()

    #Self Join
    df_self_join = df1.join(df1, df1.id == df1.id, "inner")
    print("Self Join:")
    df_self_join.show()
    
    spark.stop()

if __name__ == "__main__":
   main()