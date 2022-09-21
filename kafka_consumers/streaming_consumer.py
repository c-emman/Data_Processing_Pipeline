# from kafka import KafkaConsumer
# from json import load, loads

# data_stream_consumer = KafkaConsumer(
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda message: loads(message),
#     auto_offset_reset='earliest'
# )

# data_stream_consumer.subscribe(topics="MyFirstKafkaTopic")

# for message in data_stream_consumer:
#     print(message)

import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

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


# stream_df.writeStream\
#     .format('console')\
#         .outputMode('update')\
#             .start()\
#                 .awaitTermination()

# This selects the value park of the kafka message and turns into a string
stream_df = stream_df.selectExpr('CAST(value as STRING)')



# Creates the Schema for the dataframe
schema_for_df = StructType([StructField("category", StringType()),
                            StructField("description", StringType()),
                            StructField("downloaded", IntegerType()),
                            StructField("follower_count", StringType()),
                            StructField("image_src", StringType()),
                            StructField("index", IntegerType()),
                            StructField("is_image_or_video", StringType()),
                            StructField("save_location", StringType()),
                            StructField("tag_list", StringType()),
                            StructField("title", StringType()),
                            StructField("unique_id", StringType()),
                            ])

# Converts the JSON file into multiple columns                             
stream_df = stream_df.withColumn("value", F.from_json(stream_df["value"], schema_for_df))\
    .select(F.col("value.*"))

# Clean the follower_count column, remove the K representing thousand and add three zeros instead
stream_df = stream_df.withColumn("follower_count",
    F.when(F.col("follower_count").like("%k"), 
    (F.regexp_replace("follower_count", "k", "").cast("int")*1000))\
        .when(F.col("follower_count").like("%M"), 
    (F.regexp_replace("follower_count", "M", "").cast("int")*1000000))\
        .when(F.col("follower_count").like("%B"), 
    (F.regexp_replace("follower_count", "B", "").cast("int")*1000000000))\
        .otherwise((F.regexp_replace("follower_count", " ", "").cast("int"))))

# Clean the location save column
stream_df = stream_df.withColumn('save_location', 
    F.when(F.col('save_location').startswith('Local save in'),
    F.regexp_replace('save_location', 'Local save in ', '')))

# Make the downloaded column a boolean
stream_df = stream_df.withColumn('downloaded', F.col('downloaded').cast('boolean'))\
    .withColumn('index', F.col('index').cast('int'))

stream_df.show()

# Different feature transformations
feature_df1 = stream_df.groupBy(stream_df.category).count().show()
feature_df2 = stream_df.select(F.max('follower_count')).show()
feature_df3 = stream_df.groupBy(stream_df.category).avg('follower_count').collect().show()

# output the messages to the console
stream_df.writeStream\
    .format('console')\
        .outputMode('update')\
            .start()\
                .awaitTermination()