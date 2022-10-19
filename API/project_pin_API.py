import server_initialization
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

# Will start zookeeper and kafka servers
server_initialization.initialise()

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='Pintrest data producer', 
    value_serializer=lambda pinmessage: dumps(pinmessage).encode("ascii")
)

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

@app.post("/pin/")
def get_db_row(item: Data):
    pinmessage = dict(item)
    producer.send(topic="MyFirstKafkaTopic", value=pinmessage)
    return item

if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)