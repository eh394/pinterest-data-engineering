from lib.data_utils import data_transformation
from lib.spark import batch

# python -m src.run_processing_batch.py

# Create our Spark session, i.e. create a connection to a Spark cluster
spark = batch()

# Read all files from the s3 bucket into a PySpark dataframe
batch_df = spark.read.json("s3a://pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c/") 

# Clean and transform data
batch_df = data_transformation(batch_df)

batch_df.collect()
batch_df = batch_df.toPandas()
print(batch_df[["unique_id", "follower_count", "tag_list", "file_type"]])
# Would it make sense to save transformed data in postgres as for streaming?

