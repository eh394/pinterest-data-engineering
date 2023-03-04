from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, from_json, lit, aggregate
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StringType, IntegerType
import os
import re

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

# Create Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

# Create Spark context
sc = SparkContext(conf=conf)

def read_aws_creds():
    with open("aws_creds.yaml", "r") as aws_creds_file:
        aws_creds = yaml.safe_load(aws_creds_file)
        return aws_creds

# Configure the setting to read from the S3 bucket
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', f"{aws_creds['accessKeyId']}")
hadoopConf.set('fs.s3a.secret.key', f"{aws_creds['secretAccessKey']}")
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session, i.e. create a connection to a Spark cluster
spark = SparkSession(sc)

# Read all files from the s3 bucket into a PySpark dataframe
batch_df = spark.read.json("s3a://pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c/") 

# DATA TRANSFORMATION
def data_transformation(df):
    
    # Clean up unique_id column
    df = df.withColumn("unique_id", udf(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else None)("unique_id"))

    # Clean up title column
    df = df.withColumn("title", udf(lambda x: None if x=="No Title Data Available" else x)("title"))

    # Clean up description column
    df = df.withColumn("description", udf(lambda x: None if x=="No description available Story format" else x)("description"))

    # Clean up the follower_count column
    df = df.withColumn("follower_count", udf(lambda x: 1000*(int(x[:-1])) if x[-1]=="k" else (1000000*(int(x[:-1])) if x[-1]=='M' else (None if x=="User Info Error" else x)))("follower_count"))  

    # Clean up tag_list column
    df = df.withColumn("tag_list", udf(lambda x: x.split(","))("tag_list")) \
        .withColumn("tag_list", udf(lambda x: None if x==["N", "o", " ", "T", "a", "g", "s", " ", "A", "v", "a", "i", "l", "a", "b", "l", "e"] else x)("tag_list"))

    # Change 'is_image_or_video' column name into file_type
    df = df.withColumnRenamed("is_image_or_video","file_type")

    # Clean up image_scr column
    df = df.withColumn("image_src", udf(lambda x: None if x=="Image src error." else x)("image_src"))

    return df

batch_df = data_transformation(batch_df)


batch_df.collect()
batch_df = batch_df.toPandas()
print(batch_df[["unique_id", "follower_count", "tag_list", "file_type"]])
# Would it make sense to save transformed data in postgres as for streaming?








