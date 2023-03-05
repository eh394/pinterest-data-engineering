
from pyspark.sql.functions import when, col, from_json, regexp_replace, split
# from pyspark.sql.types import StructType, StringType, IntegerType

# Function that cleans and transforms pinterest data
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
                       .withColumn("follower_count",(col("follower_count")).cast("int"))
    
    # Replace invalid entries in the tag_list column with nulls and convert string to a list    
    df = df.withColumn("tag_list",
                       when(df.tag_list.isin(Tag_Error)==True,None) \
                        .otherwise(split(df.tag_list,",")))
    
    # Change is_image_or_video column name to file_type
    df = df.withColumnRenamed("is_image_or_video","file_type")

    # Replace invalid entries in the image_src column with nulls
    df = df.withColumn("image_src",
                       when(df.image_src.isin(Src_Error)==True,None) \
                       .otherwise(df.image_src)) 

    return df


# Sample Code using aggregations
# stream_df = stream_df.groupBy("category").count().orderBy(col("count").desc()).limit(1)
# stream_df = stream_df.groupBy("is_image_or_video").count().orderBy(col("count").desc()).limit(1)
