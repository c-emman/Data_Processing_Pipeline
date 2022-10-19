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

# Kafka bootstrap server
kafka_bootstrap_servers = 'localhost:9092'

class Spark_Stream:
    def __init__(self) -> None:
        # Create the spark session 
        self.spark =  SparkSession.builder\
            .appName('KafkaStreaming')\
                .getOrCreate()

    def kafka_conn(self):
        self.stream_df = self.spark\
            .readStream\
                .format('kafka')\
                    .option('kafka.bootstrap.servers', kafka_bootstrap_servers)\
                        .option('subscribe', kafka_topic_name)\
                            .option('startingOffsets', 'earliest')\
                                .load()

    def dataframe_schema(self):

        # This selects the value park of the kafka message and turns into a string
        self.stream_df = self.stream_df.selectExpr('CAST(value as STRING)')

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
        self.stream_df = self.stream_df.withColumn("value", F.from_json(self.stream_df["value"], schema_for_df))\
            .select(F.col("value.*"))

    def clean_stream(self):

        # Clean the follower_count column, remove the K representing thousand and add three zeros instead
        self.stream_df = self.stream_df.withColumn("follower_count",
            F.when(F.col("follower_count").like("%k"), 
            (F.regexp_replace("follower_count", "k", "").cast("int")*1000))\
                .when(F.col("follower_count").like("%M"), 
            (F.regexp_replace("follower_count", "M", "").cast("int")*1000000))\
                .when(F.col("follower_count").like("%B"), 
            (F.regexp_replace("follower_count", "B", "").cast("int")*1000000000))\
                .otherwise((F.regexp_replace("follower_count", " ", "").cast("int"))))

        # Clean the location save column
        self.stream_df = self.stream_df.withColumn('save_location', 
            F.when(F.col('save_location').startswith('Local save in'),
            F.regexp_replace('save_location', 'Local save in ', '')))

        # Make the downloaded column a boolean
        self.stream_df = self.stream_df.withColumn('downloaded', F.col('downloaded').cast('boolean'))\
            .withColumn('index', F.col('index').cast('int'))

    def feature_transforms(self):
        # Different feature transformations
        self.feature_df1 = self.stream_df.groupBy(self.stream_df.category).count()
        self.feature_df2 = self.stream_df.select(F.max('follower_count'))
        self.feature_df3 = self.stream_df.groupBy(self.stream_df.category).avg('follower_count').collect()

    def output_to_console(self):
        # output the messages to the console
        self.stream_df.writeStream\
            .format('console')\
                .outputMode('update')\
                    .start()\
                        .awaitTermination()
        self.feature_df1.writeStream\
            .format('console')\
                .outputMode('update')\
                    .start()\
                        .awaitTermination()
        self.feature_df2.writeStream\
            .format('console')\
                .outputMode('update')\
                    .start()\
                        .awaitTermination()
                        
        self.feature_df3.writeStream\
            .format('console')\
                .outputMode('update')\
                    .start()\
                        .awaitTermination()

    def run_stream(self):
        self.kafka_conn()
        self.dataframe_schema()
        self.clean_stream()
        self.output_to_console()

if __name__=='__main__':
    spark_stream_session = Spark_Stream()
    spark_stream_session.run_stream()