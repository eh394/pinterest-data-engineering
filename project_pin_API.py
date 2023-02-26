from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaClient
from kafka.cluster import ClusterMetadata
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata
from json import dumps


app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


# PYTHON-KAFKA CODE
# Create a connection to retrieve metadata
meta_cluster_conn = ClusterMetadata(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
)

# Create a connection to our KafkaBroker to check if it is running
client_conn = KafkaClient(
    bootstrap_servers="localhost:9092", # Specific the broker address to connect to
    client_id="Broker test" # Create an id from this client for reference
)

# Check that the server is connected and running
print(client_conn.bootstrap_connected())

# Create a new Kafka client to adminstrate our Kafka broker
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)

# # topics must be passed as a list to the create_topics method
# topics = []
# topics.append(NewTopic(name="Pinterest", num_partitions=1, replication_factor=1))

# # Topics to create must be passed as a list
# admin_client.create_topics(new_topics=topics)

pinterest_producer = KafkaProducer(
                bootstrap_servers="localhost:9092",
                client_id="pinterest data producer",
                value_serializer=lambda message: dumps(message).encode("ascii")
            )


# def run_infinite_post_data_loop():
#     while True:
#         sleep(random.randrange(0, 2))
#         random_row = random.randint(0, 11000)
#         connection = new_connector.create_db_connector() # had to add this line
#         selected_row = connection.execute(sqlalchemy.text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))
#         for row in selected_row.mappings().all(): # had to modify this line, read up 
#             result = dict(row)
#             pinterest_producer.send(topic='Pinterest', value=result)
#         connection.close()


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    pinterest_producer.send(topic='Pinterest', value=data)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)