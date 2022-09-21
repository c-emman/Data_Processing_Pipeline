from kafka import KafkaConsumer
from json import load, loads

data_stream_consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message: loads(message),
    auto_offset_reset='earliest'
)

data_stream_consumer.subscribe(topics="MyFirstKafkaTopic")

for message in data_stream_consumer:
    print(message)

import os
from pyspark.sql import SparkSession

# Download spark sql kafka package from Maven repository
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
# Kafka topic
kafka_topic_name = 'MyFirstKafkaTopic'

kafka_bootstrap_servers = 'localhost:9092'

spark =  SparkSession.builder\
    .appName('KafkaStreaming')\
        .getOrCreate()

stream_df = spark\
    .readStream\
        .format('kafka')\
            .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
                .option('subscribe', kafka_topic_name)\
                    .option('startingOffsets', 'earliest')\
                        .load()

# This selects the value park of the kafka message and turns into a string
stream_df = stream_df.selectExpr('CAST(value as STRING')

# output the messages to the console
stream_df.writeStream\
    .format('console')\
        .outputMode('append')\
            .start()\
                .awaitTermination()
