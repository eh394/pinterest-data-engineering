from lib.utils import transform_pinterest_data
from lib.spark import batch

spark_session = batch()
s3_bucket_id = "s3a://pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c/"

# Read all files from the s3 bucket into a PySpark dataframe
batch_df = spark_session.read.json(s3_bucket_id)
batch_df = transform_pinterest_data(batch_df)
batch_df.collect()
batch_df = batch_df.toPandas()

print(batch_df[["unique_id", "follower_count", "tag_list", "file_type"]])
