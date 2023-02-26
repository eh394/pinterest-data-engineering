from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf
import pandas as pd
from pyspark import SparkContext, SparkConf
import os
import re

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

# Create Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

# Create Spark context
sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId="AKIAY3F3BNK2GOW6Q2NS" # ideally this would not be hard-coded here, amend to read from file and use .gitignore to keep credentials file local
secretAccessKey="eD9hHGCSSiwTT4gx51nKJJd4ODhMfgXDTB3WzZvv"
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session, i.e. create a connection to a Spark cluster
spark=SparkSession(sc)

# Read all files from the s3 bucket into a PySpark dataframe
df = spark.read.json("s3a://pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c/") 

# DATA TRANSFORMATION
# Verify that uuid follows the required format
df = df.withColumn("unique_id", udf(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else None)("unique_id"))

# Convert follower count into numeric values
df = df.withColumn("follower_count", udf(lambda x: 1000*(int(x[:-1])) if x[-1]=="k" else x)("follower_count"))  # TBD: address other conditions such as users in millions or 'User Info Error'

# Convert tag_list from string into a list of strings
df = df.withColumn("tag_list", udf(lambda x: x.split(","))("tag_list")) # address condition where no tags are available (might need to find an example file) -> [N, o,  , T, a, g, s,  , A, v, a, i, l, a, b, l, e]

# Change 'is_image_or_video' column name into file_type
df = df.withColumnRenamed("is_image_or_video","file_type")

# df = df.filter(df["category"] == "art")
df.collect()
df = df.toPandas()
print(df[["unique_id", "follower_count", "tag_list", "file_type"]])
# Verify if the above is the most efficient way of carrying out transformations in spark. Would make sense to direct and save transformed data somewhere else








# import findspark
# findspark.init()
# print(findspark.find())


# # The following code maps the lambda expression to just the first element of each tuple but keeps the others in the output:
# rdd.map(lambda x: (x[0]+1, x[1], x[2])) # performs an operation on all RDD values
# # works if RDD is a list of tuples
# rdd.filter(lambda x: x is not None) # removes null values from RDD
# rdd_filtered = rdd_transformation.filter(lambda x: x[2] > 80) # removes rows unless grade is bigger than 80
# rdd_filtered.collect() # prints contents
# rdd.filter(lambda x: x is not None).collect() # collect() allows to view the contents of an RDD
# sum_gpa = rdd_transformation.map(lambda x: x[2]).reduce(lambda x,y: x + y)
# sum_gpa / rdd_transformation.count()
# ## YOUR SOLUTION HERE ##
# rdd_broadcast = rdd_transformation.map(lambda x: (x[0],x[1],x[2],broadcastStates.value[x[3]]))
# # confirm transformation is correct
# rdd_broadcast.collect()