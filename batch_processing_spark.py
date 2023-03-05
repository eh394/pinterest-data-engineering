from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, col, from_json, regexp_replace, split
import pandas as pd
from pyspark import SparkContext, SparkConf
# from pyspark.sql.types import StructType, StringType, IntegerType
import os
# import re
from pyspark_utils import data_transformation

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
aws_creds = read_aws_creds()

# Configure the setting to read from the S3 bucket
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', f"{aws_creds['accessKeyId']}")
hadoopConf.set('fs.s3a.secret.key', f"{aws_creds['secretAccessKey']}")
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session, i.e. create a connection to a Spark cluster
spark = SparkSession(sc)

# Read all files from the s3 bucket into a PySpark dataframe
batch_df = spark.read.json("s3a://pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c/") 

# Clean and transform data
batch_df = data_transformation(batch_df)

batch_df.collect()
batch_df = batch_df.toPandas()
print(batch_df[["unique_id", "follower_count", "tag_list", "file_type"]])
# Would it make sense to save transformed data in postgres as for streaming?

