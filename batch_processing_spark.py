from pyspark.sql import SparkSession
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

# DATA TRANSFORMATION
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

batch_df = data_transformation(batch_df)


batch_df.collect()
batch_df = batch_df.toPandas()
print(batch_df[["unique_id", "follower_count", "tag_list", "file_type"]])
# Would it make sense to save transformed data in postgres as for streaming?

