import findspark
import os
findspark.init(os.environ["SPARK_HOME"])
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from cassandra.cluster import Cluster

# Load the environment variables from the .env file
load_dotenv()

# Adding the necessary packages from Maven to get data from S3
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"

class Spark_DAG:

    def __init__(self) -> None:
        # Create the Spark configuration
        conf = SparkConf()\
            .setAppName('S3toSpark') \
                .setMaster('local[*]')

        sc=SparkContext(conf=conf)

        # Configuration for the setting to read from the S3 bucket
        accessKeyId=os.environ["AWS_ACCESS_KEY"]
        secretAccessKey=os.environ["AWS_SECRET_ACCESS_KEY"]
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', accessKeyId)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        # Enables the package to authenticate with AWS
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

        # Create out Spark session
        self.spark=SparkSession(sc)

    def read_data_s3_spark(self):

        # Read from the S3 bucket
        self.df = self.spark.read.option('multiline', 'true').json('s3a://pintrest-data/*.json')

    # Clean the data 

    def clean_data_from_s3(self):

        # Clean the follower_count column, remove the K representing thousand and add three zeros instead
        self.df = self.df.withColumn("follower_count",
            F.when(F.col("follower_count").like("%k"), 
            (F.regexp_replace("follower_count", "k", "").cast("int")*1000))\
                .when(F.col("follower_count").like("%M"), 
            (F.regexp_replace("follower_count", "M", "").cast("int")*1000000))\
                .when(F.col("follower_count").like("%B"), 
            (F.regexp_replace("follower_count", "B", "").cast("int")*1000000000))\
                .otherwise((F.regexp_replace("follower_count", " ", "").cast("int"))))

        # Fix the is image or video column. If Video have either V or Video and if Image I or Image. 
        # Also Image SRC should either be a link or a null value.
        self.df = self.df.withColumn('image_src', 
            F.when(F.col('image_src').startswith('https'),F.col('image_src'))\
                .otherwise(None))

        # Clean the location save column
        self.df = self.df.withColumn('save_location', 
            F.when(F.col('save_location').startswith('Local save in'),
            F.regexp_replace('save_location', 'Local save in ', '')))

        # Clean the tag list column to be null when no taglist available
        self.df = self.df.withColumn('tag_list', 
            F.when(F.col('tag_list').startswith('N,o, ,T,a,g,s,'), None)\
                .otherwise(F.col('tag_list')))

        # Make the downloaded column a boolean
        self.df = self.df.withColumn('downloaded', F.col('downloaded').cast('boolean'))\
            .withColumn('index', F.col('index').cast('int'))
        
        # Clean the image or video column
        self.df = self.df.withColumn("is_image_or_video",
            F.when(F.col("is_image_or_video").like("multi%"), "multi-video")\
                .otherwise(F.col("is_image_or_video")))

        # Replace empty values with null values
        self.df = self.df.select([F.when(F.col(c)=="",None).otherwise(F.col(c)).alias(c) for c in self.df.columns])

        self.df.show()
        #self.df.select('is_image_or_video').distinct().show(truncate=False)
        self.df.printSchema()

    def cassandra_upload(self):
        # Connect to cluster for cassandra 
        cluster = Cluster()
        session = cluster.connect()
        # Run executable statememnt to create a Keyspace
        session.execute("CREATE KEYSPACE IF NOT EXISTS pinterest_pipeline_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        # Run executable statememnt to create a Table
        session.execute("""CREATE TABLE IF NOT EXISTS pinterest_pipeline_data.pinterest_data (
                                category varchar,
                                description varchar,
                                downloaded boolean,
                                follower_count int,
                                image_src varchar,
                                "index" int,
                                is_image_or_video varchar,
                                save_location varchar,
                                tag_list varchar,
                                title varchar,
                                unique_id uuid PRIMARY KEY,
                                );""")
        
        # Close the connection
        session.shutdown()

        # Write Dataframe to Cassandra
        self.df.write.format('org.apache.spark.sql.cassandra')\
            .mode('append')\
                    .option('spark.cassandra.connection.host', '127.0.0.1')\
                        .option('spark.cassandra.connection.port', '9042')\
                            .option('keyspace', 'pinterest_pipeline_data')\
                                .option('table', 'pinterest_data', )\
                                    .save()

    def run_s3_to_spark_to_cassandra(self):
        self.read_data_s3_spark()
        self.clean_data_from_s3()
        self.cassandra_upload()

if __name__== '__main__':
    spark_job = Spark_DAG()
    spark_job.run_s3_to_spark_to_cassandra()