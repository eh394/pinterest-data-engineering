from lib.utils import transform_pinterest_data
from lib.spark import SparkConnector
import pandas as pd


env_packages = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

spark_session = (
    SparkConnector(env_packages, "S3toSpark")
    .set_conf_master("local[*]")
    .init_context()
    .set_context_hadoop_aws_conf("aws_creds")
    .get_session()
)


s3_bucket_id = "s3a://pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c/"

# Read all files from the s3 bucket into a PySpark dataframe
batch_df = spark_session.read.json(s3_bucket_id)
batch_df = transform_pinterest_data(batch_df)
batch_df.collect()
batch_df = batch_df.toPandas()
batch_df.to_json("test.json")

print(batch_df[["unique_id", "follower_count", "tag_list", "file_type"]])
