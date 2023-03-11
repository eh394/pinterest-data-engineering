from pyspark.sql.functions import when, col, regexp_replace, split
import yaml


def read_yaml_creds(filename):
    with open(f"./config/{filename}.yaml", "r") as creds:
        creds = yaml.safe_load(creds)
        return creds


# Function that cleans and transforms pinterest data
def transform_pinterest_data(df):
    # Define error states
    test_uuid = '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
    error_title = ["No Title Data Available"]
    error_description = ["No description available",
                         "No description available Story format"]
    error_follower = ["User Info Error"]
    error_tag = ["N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"]
    error_src = ["Image src error."]

    # Replace invalid unique_id column entries with nulls
    df = df.withColumn("unique_id",
                       when(df.unique_id.rlike(test_uuid) == False, None)
                       .otherwise(df.unique_id))
    # how can you make sure the id values remain unique as you upload records into postgres?

    # Replace invalid entries in the title column with nulls
    df = df.withColumn("title",
                       when(df.title.isin(error_title) == True, None)
                       .otherwise(df.title))

    # Replace invalid entries in the description column with nulls
    df = df.withColumn("description",
                       when(df.description.isin(error_description) == True, None)
                       .otherwise(df.description)) \

    # Convert follow_count entries to numeric and replace invalid entries with nulls
    df = df.withColumn("follower_count",
                       when(df.follower_count.endswith("k"),
                            regexp_replace(df.follower_count, "k", "000"))
                       .when(df.follower_count.endswith("M"), regexp_replace(df.follower_count, "M", "000000"))
                       .when(df.follower_count.isin(error_follower) == True, None)
                       .otherwise(df.follower_count)) \
        .withColumn("follower_count", (col("follower_count")).cast("int"))

    # Replace invalid entries in the tag_list column with nulls and convert string to a list
    df = df.withColumn("tag_list",
                       when(df.tag_list.isin(error_tag) == True, None)
                       .otherwise(split(df.tag_list, ",")))

    # Change is_image_or_video column name to file_type
    df = df.withColumnRenamed("is_image_or_video", "file_type")

    # Replace invalid entries in the image_src column with nulls
    df = df.withColumn("image_src",
                       when(df.image_src.isin(error_src) == True, None)
                       .otherwise(df.image_src))

    return df
