import os
from pyspark.sql import SparkSession

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "pythontokafka"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")

# outputting the messages to the console 
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()



# Define two features and one way in which data needs to be cleaned
# (1) convert tags string into a list and compute three most popular tags
# (2)




# import findspark
# findspark.init()
# import multiprocessing
# import pyspark
# from pyspark.streaming import StreamingContext

# session = pyspark.sql.SparkSession.builder.config(
#     conf=pyspark.SparkConf()
#     .setMaster(f"local[{multiprocessing.cpu_count()}]")
#     .setAppName("TestApp")
# ).getOrCreate()

# ssc = StreamingContext(session.sparkContext, batchDuration=30) # default batch duration is 0.5s 

# lines = ssc.socketTextStream("localhost", 9999)
# unique_words = lines.flatMap(lambda  text: text.split()).countByValue()
# unique_words.pprint()

# ssc.start()

# seconds = 10
# ssc.awaitTermination(seconds)

# ssc.end()

# # nothing runs until ssc.start(), we finish with ssc.end()