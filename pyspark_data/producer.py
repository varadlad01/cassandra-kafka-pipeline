from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from pyspark.sql import DataFrame
import time
import logging
from datetime import datetime

#Variable declaration

#cassandra database detail
KEYSPACE = "ineuron"
TABLE="employee"
TABLE2="prev_time"

#kafka server detail
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
KAFKA_TOPIC = "employee"

#Cassandra database connectivity credentails
CASSANDRA_HOST="cassandra"
CASSANDRA_USER="cassandra"
CASSANDRA_PASSWORD="cassandra"

#Maining log 
#log file name
LOG_FILE_NAME = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"
#log directory
LOG_FILE_DIR = os.path.join(os.getcwd(),"logs")
#create folder if not available
os.makedirs(LOG_FILE_DIR,exist_ok=True)

#create spark session with cassandra configuration
sparkSession = (SparkSession.builder
                 .config("spark.cassandra.connection.host","cassandra")
                 .config("spark.cassandra.auth.username","cassandra")
                 .config("spark.cassandra.auth.password","cassandra")
                 .appName("demo").getOrCreate()
                 )

#Reading table from cassandra db and returning spark dataframe
def dataFrameFromCassandaDbTable(sparkSession:SparkSession,keyspace:str,table:str)->DataFrame:
    df = (sparkSession.read
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load()
        #.where(col("emp_id") == 3)
        ) 
    
    # Create an empty DataFrame with a single column 'prev_ts'
    schema = StructType([StructField('prev_ts', TimestampType(), False)])
    empty_df = sparkSession.createDataFrame([], schema)
    
    # Compute the maximum value of write_ts column
    max_ts = df.agg(max("write_ts").alias("prev_ts")).collect()[0]["prev_ts"]

    # Create a DataFrame with the maximum timestamp value
    max_ts_df = sparkSession.createDataFrame([(max_ts,)], schema)
    max_ts_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table=TABLE2, keyspace=keyspace) \
    .save()
    return df

def dataFrameFromCassandaDbTable2(sparkSession:SparkSession,keyspace:str,table:str)->DataFrame:
    df2 = (sparkSession.read
        .format("org.apache.spark.sql.cassandra")
        .options(table=TABLE2, keyspace=keyspace)
        .load()
        ) 
    schema2 = StructType([StructField('prev_ts', TimestampType(), False)])
    if df2.count()>1:
        max_ts2 = df2.orderBy(col("prev_ts").desc()).collect()[1]["prev_ts"]
    else:
        max_ts2 = df2.orderBy(col("prev_ts").desc()).collect()[0]["prev_ts"]
    # Create a DataFrame with the maximum timestamp value
    max_ts_df2 = sparkSession.createDataFrame([(max_ts2,)], schema2)
    return max_ts_df2

def sendDataToKafkaTopic(kafkaBootstrapServer,topicName,dataFrame:DataFrame):
    #logging.info(f"Started writing data to kafka topic {topicName} and server: {kafkaBootstrapServer}")
    dataFrame = dataFrame.select(col("emp_id").cast(StringType()).alias("key"),to_json(struct("emp_id","emp_name","city","state","write_ts")).alias("value"))
    #dataFrame.show(10,truncate=False)
    (dataFrame
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers",kafkaBootstrapServer)
    .option("failOnDataLoss", "false") 
    .option("topic",topicName ) 
    .save())
    #logging.info(f"Data has been written to kafka topic: {topicName}")


if __name__=="__main__":
    #while True:
    # Read data from cassandra database
    df = dataFrameFromCassandaDbTable(sparkSession=sparkSession,table=TABLE,keyspace=KEYSPACE)
    
    df2 = dataFrameFromCassandaDbTable2(sparkSession=sparkSession,table=TABLE2,keyspace=KEYSPACE)
    result_df = df.join(df2).where(col("write_ts")>col("prev_ts"))

    nRows = result_df.count()
    #columns = result_df.columns

    if nRows==0:
        logging.info(f"No data found hence data will not be written to kafka topic") 
    else:
        sendDataToKafkaTopic(kafkaBootstrapServer=KAFKA_BOOTSTRAP_SERVER,
                                topicName=KAFKA_TOPIC,
                                dataFrame=result_df)
    #time.sleep(150)