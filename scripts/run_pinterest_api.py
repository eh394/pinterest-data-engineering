from json import dumps
import uvicorn
from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel

pinterest_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="pinterest data producer",
    value_serializer=lambda message: dumps(message).encode("ascii")
)

app = FastAPI()


class PinterestData(BaseModel):
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


@app.post("/pin/")
def get_db_row(item: PinterestData):
    data = dict(item)
    pinterest_producer.send(topic='Pinterest', value=data)
    return True


if __name__ == '__main__':
    uvicorn.run(app, host="localhost", port=8000)
