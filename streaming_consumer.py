from kafka import KafkaConsumer
from json import loads

# NEW
import os
from pyspark.sql import SparkSession

# NEW
# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "Pinterest"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'


# # create our consumer to retrieve the message from the topics
# streaming_consumer = KafkaConsumer(
#     bootstrap_servers="localhost:9092",    
#     value_deserializer=lambda message: loads(message),
#     auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
# )

# streaming_consumer.subscribe(topics=["Pinterest"])


# NEW
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



# # Loops through all messages in the consumer and prints them out individually
# for message in streaming_consumer:
#     print(message)