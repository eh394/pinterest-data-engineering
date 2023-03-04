from kafka import KafkaConsumer
from json import loads

# NEW
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, lit, aggregate
# regexp_extract --> find out if this could be useful 
from pyspark.sql.types import StructType, StringType, IntegerType
import re


# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.4 pyspark-shell'

# --driver-class-path ~/Desktop/dev2/pinterest/postgresql-42.5.4.jar
# org.postgresql:postgresql:42.5.4

# specify the topic we want to stream data from.
kafka_topic_name = "Pinterest"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

# Create Spark session
spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .config("spark.driver.extraClassPath", "~/Desktop/postgresql-42.5.4.jar") \
        .getOrCreate()

# .config("spark.driver.extraClassPath", sparkClassPath) \
# .config("spark.jars", "~/Desktop/dev2/pinterest/postgresql-42.5.4.jar") \


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
stream_df = stream_df.select("value")

# Create a schema
stream_df_schema = (
    StructType()
    .add("category", StringType()) 
    .add("index", IntegerType()) 
    .add("unique_id", StringType()) 
    .add("title", StringType()) 
    .add("description", StringType()) 
    .add("follower_count", StringType()) 
    .add("tag_list", StringType()) 
    .add("is_image_or_video", StringType()) 
    .add("image_src", StringType()) 
    .add("downloaded", IntegerType()) 
    .add("save_location", StringType()) 
)

stream_df = stream_df.select(
    from_json(col("value"), stream_df_schema).alias("sample")
)


stream_df = stream_df.select("sample.*")

# Clean up unique_id column
stream_df = stream_df.withColumn("unique_id", udf(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else None)("unique_id"))

# Clean up title column
stream_df = stream_df.withColumn("title", udf(lambda x: None if x=="No Title Data Available" else x)("title"))

# Clean up description column
stream_df = stream_df.withColumn("description", udf(lambda x: None if x=="No description available Story format" else x)("description"))

# Clean up the follower_count column
stream_df = stream_df.withColumn("follower_count", udf(lambda x: 1000*(int(x[:-1])) if x[-1]=="k" else (1000000*(int(x[:-1])) if x[-1]=='M' else (None if x=="User Info Error" else x)))("follower_count"))  

# Clean up tag_list column
stream_df = stream_df.withColumn("tag_list", udf(lambda x: x.split(","))("tag_list"))
stream_df = stream_df.withColumn("tag_list", udf(lambda x: None if x==["N", "o", " ", "T", "a", "g", "s", " ", "A", "v", "a", "i", "l", "a", "b", "l", "e"] else x)("tag_list"))

# Change 'is_image_or_video' column name into file_type
stream_df = stream_df.withColumnRenamed("is_image_or_video","file_type")

# Clean up image_scr column
stream_df = stream_df.withColumn("image_src", udf(lambda x: None if x=="Image src error." else x)("image_src"))


# Sample Code using aggregations
# stream_df = stream_df.groupBy("category").count().orderBy(col("count").desc()).limit(1)
# stream_df = stream_df.groupBy("is_image_or_video").count().orderBy(col("count").desc()).limit(1)

# stream_df = stream_df.select("image_src")


def for_each_batch_function(df, epoch_id):
    # df.printSchema()
    df.show()
    df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/pinterest_data") \
    .option("dbtable", "pinterest") \
    .option("user", "postgres") \
    .option("password", "Chadstone2610") \
    .save()


# .mode("overwrite") \


# outputting the messages to the console 
stream_df.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(for_each_batch_function) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

    # .foreachBatch(for_each_batch_function) \
# use "complete" output mode for aggregations

# # Starting Kafka
# ./bin/kafka-server-start.sh ./config/server.properties

# # Starting Zookeeper
# ./bin/zookeeper-server-start.sh ./config/zookeeper.properties

# # outputting the messages to the console 
# stream_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()