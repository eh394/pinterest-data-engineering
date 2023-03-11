from kafka import KafkaConsumer
from json import loads, dump
import tempfile
import boto3



# create our consumer to retrieve the message from the topics
batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message), # the message is a kafka class, message.value is a dictionary here
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

batch_consumer.subscribe(topics=["Pinterest"])


s3_client = boto3.client('s3')

# Loops through all messages in the consumer and prints them out individually
# should write a condition that if these already exist the code does not execute. Also ideally files would not be stored locally, but saved as JSON directly in the s3.
# counter = 0
# for message in batch_consumer:
#     with open(f'file_{counter}.json', 'w') as json_file:
#         dump(message.value, json_file)
#     response = s3_client.upload_file(f'file_{counter}.json', 'pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c', f'file_{counter}.json')
#     counter += 1


counter = 0
for message in batch_consumer:
    tfile = tempfile.NamedTemporaryFile(mode="w+")
    dump(message.value, tfile)
    tfile.flush()
    response = s3_client.upload_file(f"{tfile.name}", 'pinterest-data-37618829-7b6c-41ce-bb70-80d47a6e490c', f'file_{counter}.json')
    counter += 1
    if counter == 10:
        break

