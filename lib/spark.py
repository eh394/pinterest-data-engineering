from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
from lib.utils import read_yaml_creds


def batch():

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

    # Create Spark configuration
    conf = SparkConf() \
        .setAppName('S3toSpark') \
        .setMaster('local[*]')

    # Create Spark context
    sc = SparkContext(conf=conf)

    aws_creds = read_yaml_creds("aws_creds")

    # Configure the setting to read from the S3 bucket
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', f"{aws_creds['accessKeyId']}")
    hadoopConf.set('fs.s3a.secret.key', f"{aws_creds['secretAccessKey']}")
    # Allows the package to authenticate with AWS
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                   'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

    # Create Spark session
    return SparkSession(sc)


def streaming():

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.4 pyspark-shell'

    # Create Spark session
    return SparkSession \
        .builder \
        .appName("KafkaStreaming") \
        .getOrCreate()
