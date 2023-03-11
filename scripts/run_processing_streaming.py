from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructType
from lib.spark import streaming
from lib.utils import transform_pinterest_data, read_yaml_creds

spark_session = streaming()

# Only display Error messages in the console.
spark_session.sparkContext.setLogLevel("ERROR")

kafka_topic_name = "Pinterest"
kafka_bootstrap_servers = 'localhost:9092'

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

# Construct a streaming DataFrame that reads from topic
stream_df = (
    spark_session
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic_name)
    .option("startingOffsets", "earliest")
    .load()
)

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")
stream_df = stream_df.select("value")
stream_df = stream_df.select(
    from_json(col("value"), stream_df_schema).alias("sample"))
stream_df = stream_df.select("sample.*")
stream_df = transform_pinterest_data(stream_df)

# Load local Postgres database credentials
db_creds = read_yaml_creds("db_creds")


# Define function writing records to the local Postgres database
def for_each_batch_function(df, _epoch_id):
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


# Sample Code using aggregations
# stream_df = stream_df.groupBy("category").count().orderBy(col("count").desc()).limit(1)
# stream_df = stream_df.groupBy("is_image_or_video").count().orderBy(col("count").desc()).limit(1)
