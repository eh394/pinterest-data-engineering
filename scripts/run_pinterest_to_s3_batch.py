import tempfile
from json import dump, loads
import boto3
from kafka import KafkaConsumer

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics=["Pinterest"])

s3_client = boto3.client("s3")
s3_bucket_id = "pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c"


def upload_pinterest_to_s3(limit):
    counter = 0
    for message in batch_consumer:
        tfile = tempfile.NamedTemporaryFile(mode="w+")
        dump(message.value, tfile)
        tfile.flush()
        _response = s3_client.upload_file(
            f"{tfile.name}",  s3_bucket_id, f"file_{counter}.json")
        counter += 1
        if counter == limit:
            break


if __name__ == "__main__":
    upload_pinterest_to_s3(limit=10)
