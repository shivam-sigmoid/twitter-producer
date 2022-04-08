# from kafka import KafkaConsumer
# import json
#
topic_name = "test-topic"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.types import *


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
#
#
# consumer = KafkaConsumer(
#     topic_name,
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='latest',
#     enable_auto_commit=True,
#     auto_commit_interval_ms=5000,
#     fetch_max_bytes=128,
#     max_poll_records=100,
#
# # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )
#
# # READ stream from kafka and convert it to dataframe
#
# # Select only value as key is NULL (b'' => "") (binary format => string format)
#
#
spark = SparkSession.builder \
    .master("local") \
    .appName("demo") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

events = df.selectExpr("CAST(value AS STRING)")
#
# # print(events)
#
# # schema = StructType([ \
# #     StructField("tweet_id", StringType(), True), \
# #     StructField("created_at", StringType(), True), \
# #     StructField("text", StringType(), True), \
# #     StructField("screen_name", StringType(), True), \
# #     StructField("user_created_at", StringType(), True), \
# #     StructField("user_location", StringType(), True), \
# #     StructField("user_id", StringType(), True), \
# #     StructField("geo", StringType(), True), \
# #     StructField("is_truncated", StringType(), True), \
# #     StructField("tweet_contributors", StringType(), True), \
# #     StructField("place", StringType(), True), \
# #     StructField("coordinates", IntegerType(), True) \
# #     ])
#
# schema = StructType([ \
#     StructField("id", StringType(), True), \
#     StructField("created_at", StringType(), True), \
#     StructField("text", StringType(), True), \
#     StructField("user", StringType(), True) \
#     ])

schema = StructType([ \
    StructField("id", StringType(), True), \
    StructField("created_at", StringType(), True), \
    StructField("text", StringType(), True), \
    StructField('user', StructType([
        StructField('location', StringType(), True)
    ]))
    ])
#
#
# # id :
# # created_at:
# # tweet:
# # location:
# # etc
# #
#
table = events.select(from_json(events.value, schema) \
    .alias("tmp")) \
    .select("tmp.*")
# # Api -> Flask -> Mongo Server
# # return group by location count
#
#
# for message in consumer:
    # tweets = json.loads(json.dumps(message.value))
    # print(message.text)
    # print(str(message.value))
    # my_json = message.value.decode('utf8').replace("'", '"')
    # print(my_json)
    # print('- ' * 20)
    # print(message.value)
#
#
# # spark-submit --class demo --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 <filename>.py
# # spark-submit --class demo --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 consumer.py
#
query = table.writeStream.foreachBatch(write_mongo_row).start()
query.awaitTermination()
#
# Write to the console

# # query = table \
# #     .writeStream \
# #     .outputMode("append") \
# #     .option("truncate", False) \
# #     .format("console") \
# #     .start() \
# #     .awaitTermination()
#
#
# # Load the JSON to a Python list & dump it back out as formatted JSON
# # data = json.loads(my_json)
# # s = json.dumps(data, indent=4, sort_keys=True)
# # print(s)
# # print(message.text.decode('utf-8'))
# # print(message.replace("\\/", "/").encode().decode('unicode_escape', 'surrogatepass'))
# # print(tweets)
#
#
# # Mongo -> Python
#
# # ConsumerRecord(topic='test-topic', partition=3, offset=125, timestamp=1649153574339, timestamp_type=0, key=None, value=b'b\'{"created_at":"Tue Apr 05 10:12:49 +0000 2022","id":1511285722774396931,"id_str":"1511285722774396931","text":"RT @drajm: Pretty sure EVERYONE I know who has #covid got it via their kid\\\\u2019s school.\\\\n\\\\nWeren\\\\u2019t the usual suspects spinning some nonsense abo\\\\u2026","source":"\\\\u003ca href=\\\\"http:\\\\/\\\\/twitter.com\\\\/download\\\\/android\\\\" rel=\\\\"nofollow\\\\"\\\\u003eTwitter for Android\\\\u003c\\\\/a\\\\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":1346211083636367360,"id_str":"1346211083636367360","name":"Michelle C","screen_name":"MichelleC2021","location":"Ireland","url":null,"description":"go placidly amid the noise and the haste, and remember what peace there may be in silence","translator_type":"none","protected":false,"verified":false,"followers_count":135,"friends_count":124,"listed_count":0,"favourites_count":16144,"statuses_count":11964,"created_at":"Mon Jan 04 21:45:26 +0000 2021","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":null,"contributors_enabled":false,"is_translator":false,"profile_background_color":"F5F8FA","profile_background_image_url":"","profile_background_image_url_https":"","profile_background_tile":false,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\\\\/\\\\/pbs.twimg.com\\\\/profile_images\\\\/1346545389479079936\\\\/2TrjSKyK_normal.jpg","profile_image_url_https":"https:\\\\/\\\\/pbs.twimg.com\\\\/profile_images\\\\/1346545389479079936\\\\/2TrjSKyK_normal.jpg","profile_banner_url":"https:\\\\/\\\\/pbs.twimg.com\\\\/profile_banners\\\\/1346211083636367360\\\\/1609871563","default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null,"withheld_in_countries":[]},"geo":null,"coordinates":null,"place":null,"contributors":null,"retweeted_status":{"created_at":"Tue Apr 05 00:47:28 +0000 2022","id":1511143447960305664,"id_str":"1511143447960305664","text":"Pretty sure EVERYONE I know who has #covid got it via their kid\\\\u2019s school.\\\\n\\\\nWeren\\\\u2019t the usual suspects spinning some\\\\u2026 https:\\\\/\\\\/t.co\\\\/geM0R6zLdz","source":"\\\\u003ca href=\\\\"http:\\\\/\\\\/twitter.com\\\\/download\\\\/iphone\\\\" rel=\\\\"nofollow\\\\"\\\\u003eTwitter for iPhone\\\\u003c\\\\/a\\\\u003e","truncated":true,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":148950709,"id_str":"148950709","name":"Dr Miller - vax kids asap","screen_name":"drajm","location":"Whadjuk Noongar land.","url":"https:\\\\/\\\\/thewest.com.au","description":"President Aust Soc of Anaesthetists, AMA WA Past Pres. Columnist at The West @WestAustralian. Twts mine. Like, RT \\\\u2260 E.","translator_type":"none","protected":false,"verified":false,"followers_count":15888,"friends_count":9749,"listed_count":142,"favourites_count":25971,"statuses_count":14188,"created_at":"Fri May 28 01:23:11 +0000 2010","utc_offset":null,"time_zone":null,"geo_enabled":true,"lang":null,"contributors_enabled":false,"
