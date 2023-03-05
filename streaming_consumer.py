from kafka import KafkaConsumer
from json import loads
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, udf, from_json, lit, aggregate, regexp_replace, split
# regexp_extract --> find out if this could be useful 
from pyspark.sql.types import StructType, StringType, IntegerType
import re
import yaml


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
# Save jar file in a more appropriate directory
#    .config("spark.driver.extraClassPath", "~/Desktop/postgresql-42.5.4.jar") \

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


def data_transformation(df):
    # Define error states
    Test_UUID = '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
    Title_Error = ["No Title Data Available"]
    Description_Error = ["No description available", "No description available Story format"]
    Follower_Error = ["User Info Error"]
    Tag_Error = ["N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"]
    Src_Error = ["Image src error."]

    # Replace invalid unique_id column entries with nulls
    df = df.withColumn("unique_id",
                       when(df.unique_id.rlike(Test_UUID)==False,None) \
                       .otherwise(df.unique_id))
    # how can you make sure the id values remain unique as you upload records into postgres?

    # Replace invalid entries in the title column with nulls
    df = df.withColumn("title",
                       when(df.title.isin(Title_Error)==True,None) \
                       .otherwise(df.title)) 
                                                    
    # Replace invalid entries in the description column with nulls
    df = df.withColumn("description",
                       when(df.description.isin(Description_Error)==True,None) \
                       .otherwise(df.description)) \
                     
    # Convert follow_count entries to numeric and replace invalid entries with nulls
    df = df.withColumn("follower_count",
                       when(df.follower_count.endswith("k"),regexp_replace(df.follower_count,"k","000")) \
                       .when(df.follower_count.endswith("M"),regexp_replace(df.follower_count,"M","000000")) \
                       .when(df.follower_count.isin(Follower_Error)==True,None) \
                       .otherwise(df.follower_count)) \
                       .withColumn("follower_count",col("follower_count").cast("int"))
    
    # Replace invalid entries in the tag_list column with nulls and convert string to a list    
    df = df.withColumn("tag_list",
                       when(df.tag_list.isin(Tag_Error)==True,None) \
                        .otherwise(split(col("tag_list"),",")))
    
    # Change is_image_or_video column name to file_type
    df = df.withColumnRenamed("is_image_or_video","file_type")

    # Replace invalid entries in the image_src column with nulls
    df = df.withColumn("image_src",
                       when(df.image_src.isin(Src_Error)==True,None) \
                       .otherwise(df.image_src)) 

    return df

stream_df = data_transformation(stream_df)
stream_df = stream_df.select('title')

# Sample Code using aggregations
# stream_df = stream_df.groupBy("category").count().orderBy(col("count").desc()).limit(1)
# stream_df = stream_df.groupBy("is_image_or_video").count().orderBy(col("count").desc()).limit(1)

def read_db_creds():
    with open("db_creds.yaml", "r") as db_creds_file:
        db_creds = yaml.safe_load(db_creds_file)
        return db_creds
db_creds = read_db_creds()

def for_each_batch_function(df, epoch_id):
    df.show()
    df.write \
    .format("jdbc") \
    .mode("append") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/pinterest_data") \
    .option("dbtable", "pinterest") \
    .option("user", f"{db_creds['USER']}") \
    .option("password", f"{db_creds['PASSWORD']}") \
    .save()

# writing transformed rows to postgres database 
stream_df.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(for_each_batch_function) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# outputting the messages to the console 
# stream_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()

