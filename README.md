# Introduction

This project aims to emulate Pinterest's data processing pipeline. It adopts a hybrid-approach lambda architecture deployment model where traditional batch and real-time stream pipelines run in parallel, thus providing comprehensive view of historic (batch) as well as more recent (stream) data to facilitate business decisions. 

In order to emulate Pinterest's current pipeline, the project employs Apache Kafka for data ingestion. In the batch pipeline, Kafka sends all data to an intermediate database, in our case AWS S3 bucket (to simulate memSQL used by Pinterest). The data can be either retained in this database for long-term persistent storage or in our case it is sent to Apache Spark for transformation. The batch transformations are orchestrated using Apache Airflow. In the stream pipeline Spark Streaming is used to transform real-life data and save them to a local Postgres database.

Use of these tools is aimed at emulating a real-life scenario where rapid growth of Pinterest's user database as well as amount of data being generated, requires a reliable and scalable data processing pipeline.
<br>
<br>

# Tools Used

Tools employed in this project include:
- Apache Kafka
- Apache Spark / Spark Streaming
- Apache Airflow
- AWS S3
- Postgres
<br>
<br>

# Code Organization

The code is organized in two main folders /lib and /scripts. 

## /lib

1. spark.py
Includes class SparkConnector used to instantiate a spark session for both, batch and stream pipelines.
2. utils.py
Includes two functions:
- transform_pinterest_data: this function performs cleaning operations on the data.
- read_yaml_creds: this function is used to retrieve credentials for AWS S3 bucket and Postrgres databases stored in a local /config folder.

## /scripts

1. Incoming Data:
- run_pinterest_api.py: FastAPI is utilised to emulate Pinterest's API.
- run_pinterest_emulation.py: Code aims at emulating user posting data, here sqalchemy is utilised to load the data from AWS database.

2. Batch Pipeline:
- run_pinterest_to_s3_batch.py: Using Kafka and boto3, the pinterest data is sent to AWS S3 bucket.
- run_processing_batch.py: Spark session is created using SparkConnector class from spark.py and data is transformed with transform_pinterest_data function from utils.py
- run_airflow_batch.py: contains DAG orchestrating batch processing.

3. Stream Pipeline:
- run_processing_streaming.py: Spark session is created using SparkConnector class from spark.py and data is transformed with transform_pinterest_data function from utils.py. Following transformation data is sent to a local Postgres database.
<br>
<br>

# Code Execution

## Batch Pipeline

There are two main stages in the batch pipeline: (1) saving data to AWS S3 and (2) batch data transformation.

### STAGE 1 - saving data to AWS S3

STEP 1 - Start Zookeeper \
./bin/zookeeper-server-start.sh ./config/zookeeper.properties

STEP 2 - Start Kafka \
./bin/kafka-server-start.sh ./config/server.properties

STEP 3 - Start API \
python run_pinterest_api.py

STEP 4 - Start User Posting Emulation \
python run_pinterest_emulation.py

STEP 5 - Start Batch Ingestion \
python run_pinterest_to_s3_batch.py \
<br>

### STAGE 2 - batch data transformation

STEP 1 - Run Batch Transformation: \
python -m scripts.run_processing_batch \
OR use Airflow:\
airflow db init \
airflow webserver --port 8080 \
Airflow orchestrates the following tasks: \
start_zookeeper >> start_kafka >> run_batch_processing >> close kafka >> close_zookeeper

## Stream Pipeline

In order for the stream pipeline to run, the following files / code need to be executed: \
STEP 1 - Start Zookeeper \
./bin/zookeeper-server-start.sh ./config/zookeeper.properties

STEP 2 - Start Kafka \
./bin/kafka-server-start.sh ./config/server.properties

STEP 3 - Start API \
python run_pinterest_api.py

STEP 4 - Start User Posting Emulation \
python run_pinterest_emulation.py

STEP 5 - Start Spark Streaming \
python -m scripts.run_processing_streaming












