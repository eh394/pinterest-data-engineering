from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import yaml


def batch():
    
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

    # Create Spark configuration
    conf = SparkConf() \
        .setAppName('S3toSpark') \
        .setMaster('local[*]')

    # Create Spark context
    sc = SparkContext(conf=conf)

    def read_aws_creds():
        with open("./config/aws_creds.yaml", "r") as aws_creds_file:
            aws_creds = yaml.safe_load(aws_creds_file)
            return aws_creds
    aws_creds = read_aws_creds()

    # Configure the setting to read from the S3 bucket
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', f"{aws_creds['accessKeyId']}")
    hadoopConf.set('fs.s3a.secret.key', f"{aws_creds['secretAccessKey']}")
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

    # Create Spark session
    return SparkSession(sc)


def streaming():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.4 pyspark-shell'

    # Create Spark session
    return SparkSession \
            .builder \
            .appName("KafkaStreaming") \
            .getOrCreate()
