from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_utc_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, MapType
from datetime import datetime
import pytz
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.0,' \
                             'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0,' \
                                    'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell '


def write_mongo_row(df, epoch_id):
    mongoURL = "mongodb://localhost:27017/"
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", mongoURL) \
        .option('database', 'twitter_db') \
        .option('collection', 'tweets') \
        .save()
    pass


def getDate(x):
    if x is not None:
        return str(
            datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"))
    else:
        return None


spark = SparkSession \
    .builder \
    .appName("From Kafka Topic") \
    .master("local[3]") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "covid_topic") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("created_at", StringType(), True),
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField('user', StructType([
        StructField('id', StringType(), True),
        StructField('screen_name', StringType(), True),
        StructField('location', StringType(), True)
    ])),
    StructField('geo', StringType(), True),
    StructField('coordinates', StringType(), True),
    StructField('place', StringType(), True),
    StructField('extended_tweet', StructType([
            StructField("full_text", StringType(), True)
    ]))
])


value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*")
value_df.printSchema()
# value_df.show()
flatten_df = value_df.selectExpr("id", "created_at", "text", "geo", "coordinates", "place", "user.id as user_id",
                                 "user.screen_name as username", "user.location as location",
                                 "extended_tweet.full_text as full_text")
flatten_df.printSchema()
# flatten_df.show()

date_fn = udf(getDate, StringType())
flatten_df = flatten_df.withColumn("created_at", to_utc_timestamp(date_fn("created_at"), "UTC")) \
                        .withColumn('date', col('created_at').cast('date')) \
                        .withColumn('full_text', coalesce(flatten_df.full_text, flatten_df.text)) \
                        .drop('created_at') \
                        .drop('text')

flatten_df.printSchema()
# flatten_df.show()

# writer_query = flatten_df.writeStream \
#                         .format("mongo") \
#                         .option("uri", "mongodb://localhost:27017/") \
#                         .queryName("Transformed Data Writer") \
#                         .outputMode("append") \
#                         .option("database", "twitter_db") \
#                         .option("collection", "tweets") \
#                         .option("checkpointLocation", "chk-point-dir") \
#                         .trigger(processingTime="1 minute") \
#                         .save()
#
# writer_query.awaitTermination()

query = flatten_df.writeStream.foreachBatch(write_mongo_row).start()
query.awaitTermination()

# query = flatten_df.writeStream \
#         .format("json") \
#         .queryName("Writer") \
#         .outputMode("append") \
#         .option("path", "output") \
#         .option("checkpointLocation", "chk-point-dir") \
#         .trigger(processingTime="1 minute") \
#         .start()

# query.awaitTermination()
