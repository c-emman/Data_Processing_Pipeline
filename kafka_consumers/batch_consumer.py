from kafka import KafkaConsumer
from json import loads, dumps
import boto3
s3_bucket = 'pintrest-data'
s3_client = boto3.client('s3')

data_batch_consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: loads(message),
    auto_offset_reset='earliest'
)

data_batch_consumer.subscribe(topics="MyFirstKafkaTopic")

for message in data_batch_consumer:
    print(message[6]["unique_id"])
    s3_client.put_object(Body=dumps(message), Bucket=s3_bucket, Key=f'{message[6]["unique_id"]}.json')
    
