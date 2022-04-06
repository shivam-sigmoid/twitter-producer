topic_name = "test-topic"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

# Writing data to MongoDB

def write_mongo_row(df, epoch_id):
    mongoURL = "mongodb://localhost:27017/"
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", mongoURL) \
        .option('database', 'twitter-db') \
        .option('collection', 'tweets') \
        .save()
    pass

# Creating Spark Session

spark = SparkSession.builder \
    .master("local") \
    .appName("demo") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .getOrCreate()

# Creating Data Frame from Kafka Consumer Stream

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Extracting value from created Dataframe

events = df.selectExpr("CAST(value AS STRING)")

# Defining the Schema

# schema = StructType([ \
#     StructField("tweet_id", StringType(), True), \
#     StructField("created_at", StringType(), True), \
#     StructField("text", StringType(), True), \
#     StructField("screen_name", StringType(), True), \
#     StructField("user_created_at", StringType(), True), \
#     StructField("user_location", StringType(), True), \
#     StructField("user_id", StringType(), True), \
#     StructField("geo", StringType(), True), \
#     StructField("is_truncated", StringType(), True), \
#     StructField("tweet_contributors", StringType(), True), \
#     StructField("place", StringType(), True), \
#     StructField("coordinates", IntegerType(), True) \
#     ])

schema = StructType([ \
    StructField("id", StringType(), True), \
    StructField("created_at", StringType(), True), \
    StructField("text", StringType(), True), \
 \
    ])

# Incorporating Schema and Value

table = events.select(from_json(events.value, schema) \
                      .alias("tmp")) \
    .select("tmp.*")

# To run the code

# spark-submit --class demo --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 consumer.py

# Mongo Sink from Spark Streaming

query = table.writeStream.foreachBatch(write_mongo_row).start()
query.awaitTermination()

# Writing to Console

# query = table \
#     .writeStream \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .format("console") \
#     .start() \
#     .awaitTermination()
