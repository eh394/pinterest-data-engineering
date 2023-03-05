import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import yaml
from pyspark_utils import data_transformation

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.4 pyspark-shell'

# Specify the topic we want to stream data from.
kafka_topic_name = "Pinterest"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

# Create Spark session
spark = SparkSession \
        .builder \
        .appName("KafkaStreaming") \
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

# Clean and transform data
stream_df = data_transformation(stream_df)

# Load local Postgres database credentials
def read_db_creds():
    with open("db_creds.yaml", "r") as db_creds_file:
        db_creds = yaml.safe_load(db_creds_file)
        return db_creds
db_creds = read_db_creds()

# Define function writing records to the local Postgres database
def for_each_batch_function(df, epoch_id):
    df.show()
    df.write \
    .format("jdbc") \
    .mode("append") \
    .option("driver", f"{db_creds['DRIVER']}") \
    .option("url", f"{db_creds['URL']}") \
    .option("dbtable", "pinterest") \
    .option("user", f"{db_creds['USER']}") \
    .option("password", f"{db_creds['PASSWORD']}") \
    .save()

# Writing transformed rows to local Postgres database 
stream_df.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(for_each_batch_function) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# # Outputting the messages to the console 
# stream_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()

