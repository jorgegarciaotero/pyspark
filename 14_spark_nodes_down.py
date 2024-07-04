from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, avg, max, min,round, sum
from pyspark.sql.functions import dayofmonth, weekofyear, month, year,unix_timestamp
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.types import *
import traceback

import logging
 


class MyLogger:
    def __init__(self, filename='app.log', level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        file_handler = logging.FileHandler(filename)
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
    
    def get_logger(self):
        return self.logger


def load_dataframe(logger,spark):
    '''
    Load the CSV file into a DataFrame.
    args:
        logger: logging object
        spark: SparkSession object
    returns:
        DataFrame object
    '''
    try:
        logger.info("Loading DataFrame...")
        # Define the data schema
        data_schema = [
            StructField("alarm_date", TimestampType(), True),
            StructField("ntr", StringType(), True),
            StructField("node", StringType(), True), 
            StructField("site_id", StringType(), True),
            StructField("bsc_rnc", StringType(), True),
            StructField("emplacement", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("operator_notes", StringType(), True),
            StructField("num_cells_down", IntegerType(), True),
            StructField("cells_down", StringType(), True),  #
            StructField("tech", StringType(), True),
            StructField("zone", StringType(), True),
            StructField("region", StringType(), True),
            StructField("county", StringType(), True),
            StructField("population", StringType(), True),
            StructField("site_address", StringType(), True),
            StructField("site_lat", DoubleType(), True),
            StructField("site_lon", DoubleType(), True),
            StructField("usertext", StringType(), True),
            StructField("province", StringType(), True),
            StructField("aacc", StringType(), True),
            StructField("num_tics", IntegerType(), True),
            StructField("ingest_time", TimestampType(), True),
            StructField("last_update", TimestampType(), True),
        ]

        final_struc = StructType(fields=data_schema)
        # Read CSV with specified options
        dataframe = spark.read.csv("/opt/bitnami/spark/data/nodos_caidos_historicos.csv",
                            sep=";",
                            header=True,
                            schema=final_struc)
        dataframe.printSchema()
        logger.info("DataFrame loaded successfully")
    except Exception as e:
        logger.error(f'Error loading DataFrame: {str(e)}', exc_info=True)
    return dataframe
        
     
     
def transform_dataframe(logger,dataframe):
    '''
    Transform the DataFrame.
    args:
        logger: logging object
        dataframe: DataFrame object
    returns:
        DataFrame object
    '''
    try:
        logger.info("Transforming DataFrame...")
        dataframe_dedup = dataframe.dropDuplicates(["alarm_date","node"])
        dataframe_transf1  = dataframe_dedup \
                .withColumn("ingest_day",dayofmonth("alarm_date"))\
                .withColumn("ingest_week",weekofyear("alarm_date"))\
                .withColumn("ingest_month",month("alarm_date"))\
                .withColumn("ingest_year",year("alarm_date"))\
                .withColumn("duration_hours", round((unix_timestamp(col("last_update")) - unix_timestamp(col("alarm_date"))) / 3600, 2))
        dataframe_transf1.select("alarm_date","last_update","duration_hours", "ingest_day", "ingest_month","ingest_year")
        logger.info("Dataframe transformed.")
    except Exception as e:
        logger.error(f'Error transforming DataFrame: {str(e)}', exc_info=True)
    return dataframe_transf1
        
 
def join_by_condition(dataframes, column_names, join_type,logger):
    '''
    Join multiple DataFrames based on the specified column names.
    Args:
        dataframes: List of DataFrame objects
        column_names: List of column names to be used for join conditions
        join_type: Type of join (e.g., "inner", "outer", "left", "right")
    Returns:
        dataframe_main: DataFrame object resulting from the join
    '''
    try:
        logger.info("Joining DataFrames...")
        # Left (first) dataframe to be joined
        dataframe_main = dataframes[0]
    
        # Iterates and joins the remain dataframes
        for i in range(1, len(dataframes)):
            dataframe_current = dataframes[i]
            # Builds the join conditions (key columns)
            join_conditions = [dataframe_main[col] == dataframe_current[col] for col in column_names]
            # Performs the join
            dataframe_main = dataframe_main.join(dataframe_current, on=join_conditions, how=join_type)
            # Stores new and dupe cols
            newcols = []
            dupcols = []
            for i in range(len(dataframe_main.columns)):
                if dataframe_main.columns[i] not in newcols:
                    newcols.append(dataframe_main.columns[i])
                else:
                    dupcols.append(i)
            # Removes dupe cols from dataframe
            dataframe_main = dataframe_main.toDF(*[str(i) for i in range(len(dataframe_main.columns))])
            for dupcol in dupcols:
                dataframe_main = dataframe_main.drop(str(dupcol))
            dataframe_main=dataframe_main.toDF(*newcols)
        logger.info("Dataframes joined.")
        return dataframe_main
    except Exception as e:
        logger.error(f'Error joining the DataFrames: {str(e)}', exc_info=True)
 




def aggregate_alarms(logger, dataframe):
    '''
    Aggregates the dataframe in various ways.
    args:   
        - logger: logging object.
        - dataframe: dataframe of data
    returns: 
        - dataframe_agg_year: dataframe of yearly agg data
        - dataframe_agg_year_month: dataframe of monthly agg data

    '''
    try:
        logger.info("Aggregating alarms...")
        #Count of the alarms for every node
        dataframe_count_by_year = dataframe.groupBy("node", "ingest_year").agg(count("*").alias("num_alarms_per_year"))
        dataframe_count_by_year_month = dataframe.groupBy("node", "ingest_year", "ingest_month").agg(count("*").alias("num_alarms_per_year_month"))

        #Sum  of duration of the alarms for every node
        dataframe_sum_duration_by_year = dataframe.groupBy("node", "ingest_year").agg(round(sum("duration_hours"),2).alias("sum_duration_hours"))
        dataframe_sum_duration_by_year_month=dataframe.groupBy("node", "ingest_year", "ingest_month").agg(round(sum("duration_hours"),2).alias("sum_duration_hours"))
        
        #Avg duration of the alarms for every node
        dataframe_avg_duration_by_year = dataframe.groupBy("node", "ingest_year").agg(round(avg("duration_hours"),2).alias("avg_duration_hours"))
        dataframe_avg_duration_by_year_month = dataframe.groupBy("node", "ingest_year", "ingest_month").agg(round(avg("duration_hours"),2).alias("avg_duration_hours"))
        
        #Total Customer incidents associated to every alarm
        dataframe_sum_tics_by_year = dataframe.groupBy("node", "ingest_year").agg(sum("num_tics").alias("total_tics"))
        dataframe_sum_tics_by_year_month = dataframe.groupBy("node", "ingest_year", "ingest_month").agg(sum("num_tics").alias("total_tics"))
        
        logger.info("Alarms aggregated. Sorting...")
        #yearly dfs
        dataframe_count_by_year = dataframe_count_by_year.orderBy("ingest_year",desc("num_alarms_per_year"))
        dataframe_sum_duration_by_year = dataframe_sum_duration_by_year.orderBy( "ingest_year",desc("sum_duration_hours"))
        dataframe_avg_duration_by_year = dataframe_avg_duration_by_year.orderBy( "ingest_year",desc("avg_duration_hours"))
        dataframe_sum_tics_by_year = dataframe_sum_tics_by_year.orderBy( "ingest_year",desc("total_tics"))
        #monthly dfs
        dataframe_count_by_year_month = dataframe_count_by_year_month.orderBy( "ingest_year", "ingest_month",desc("num_alarms_per_year_month"))
        dataframe_sum_duration_by_year_month = dataframe_sum_duration_by_year_month.orderBy( "ingest_year", "ingest_month",desc("sum_duration_hours"))
        dataframe_avg_duration_by_year_month = dataframe_avg_duration_by_year_month.orderBy( "ingest_year", "ingest_month",desc("avg_duration_hours"))
        dataframe_sum_tics_by_year_month = dataframe_sum_tics_by_year_month.orderBy( "ingest_year", "ingest_month",desc("total_tics"))
        
        logger.info("Alarms sorted. Joining...")
        dataframes_year=[dataframe_count_by_year,dataframe_sum_duration_by_year,dataframe_avg_duration_by_year,dataframe_sum_tics_by_year]
        column_names_year=["node","ingest_year"]
        condition="left"
        dataframe_agg_year=join_by_condition(dataframes_year,column_names_year,condition,logger)        
        
        dataframes_month=[dataframe_count_by_year_month,dataframe_sum_duration_by_year_month,dataframe_avg_duration_by_year_month,dataframe_sum_tics_by_year_month]
        column_names_month=["node","ingest_year","ingest_month"]    
        dataframe_agg_year_month=join_by_condition(dataframes_month,column_names_month,condition,logger)               
          
        return dataframe_agg_year, dataframe_agg_year_month                
                                                      
    except Exception as e:
        logger.error(f'Error counting alarms: {str(e)}', exc_info=True)

   

def adding_node_info_to_alarm(logger,dataframe,dataframe_aggregated):
    '''
    Adds node info to the aggregated dataframe of alarms
    args:
        - logger: logging object.
        - dataframe: dataframe of data
        - dataframe_aggregated: dataframe of aggregated data
    returns:
        - dataframe_aggregated: dataframe of aggregated data with node info
    '''
    try:
        dataframe_dedup = dataframe.dropDuplicates(["node"])\
            .select("node","site_id","bsc_rnc","emplacement","provider","zone","population","site_lat","site_lon",
                    "province","aacc")
        column_names=["node"]
        condition="left"
        dataframes=[dataframe,dataframe_aggregated]
        dataframe_joined=join_by_condition(dataframes,column_names,condition,logger) 
        return dataframe_joined
    except Exception as e:
        logger.error(f'Error adding node info to the aggregated dataframe: {str(e)}', exc_info=True)
    
    




def main():

    logger = MyLogger(filename='/opt/bitnami/spark/logs/app.log', level=logging.INFO).get_logger()
    
    spark = SparkSession.builder\
        .appName("datafraud_sandbox")\
        .config("spark.sql.debug.maxToStringFields", 100)\
        .getOrCreate()  
    
    dataframe = load_dataframe(logger,spark)
    
    dataframe_transf1 = transform_dataframe(logger,dataframe)
    
    dataframe_agg_year, dataframe_agg_year_month  = aggregate_alarms(logger, dataframe_transf1)

    print("DATAFRAMES RESULT")
    dataframe_agg_year.show()
    dataframe_agg_year_month.show() 
    
  
    
    dataframe_agg_year_info=adding_node_info_to_alarm(logger,dataframe,dataframe_agg_year)
    dataframe_agg_year_month_info=adding_node_info_to_alarm(logger,dataframe,dataframe_agg_year_month)

    dataframe_agg_year_info.coalesce(1).write.mode("overwrite").csv('/opt/bitnami/spark/data/dataframe_agg_year.csv', header=True, sep=';')
    dataframe_agg_year_month_info.coalesce(1).write.mode("overwrite").csv('/opt/bitnami/spark/data/dataframe_agg_year_month.csv', header=True, sep=';')

    dataframe_agg_year_info.show()
    dataframe_agg_year_month_info.show()
    
    spark.stop()  

if __name__ == "__main__":
   main()