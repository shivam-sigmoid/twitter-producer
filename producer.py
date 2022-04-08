import tweepy
import json
from bson import json_util

consumer_key = "siIbnTn8GcBUsfySy0VmjyR7A"
consumer_secret = "PBH7bOVRrv7gEiksWhvczVdFYjiHLIogv3RxQHpfV65gfjI36F"
access_token = "1510924415642791938-DXjvrckvFFT05fiAXQ5oUDmmrqpN98"
access_token_secret = "VOuZDj8EMGbyfQx2Aen4h7FLAsuxSsZmp4Ry5WanCUni8"

# from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])  # Same port as your Kafka server

topic_name = "test-topic"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth


class TwitterStreamer():
    """SET UP STREAMER"""

    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            # listener = ListenerTS()
            # auth = self.twitterAuth.authenticateTwitterApp()
            # stream = Stream(auth, listener)
            stream = ListenerTS(consumer_key, consumer_secret, access_token, access_token_secret)
            stream.filter(track=["covid", "corona", "covid-19"], stall_warnings=True, languages=["en"])


# class ListenerTS(StreamListener):
class ListenerTS(tweepy.Stream):
    def on_data(self, raw_data):
        # my_json = raw_data.decode('utf8').replace("'", '"')
        # data = json.loads(my_json)
        # s = json.dumps(data, indent=4, sort_keys=True)
        # print(s)

        print(raw_data)

        # raw_data = str(raw_data)
        producer.send(topic_name, raw_data)

        # producer.send(topic_name, str.encode(raw_data))
        # d = {
        #     "id":raw_data.id,
        #     "created_at":raw_data.created_at
        # }
        # producer.send(topic_name, json.dumps(d, default=json_util.default).encode('utf-8'))
        # print(raw_data)
        # producer.send(topic_name,raw_data.encode('utf-8'))
        # producer.send(topic_name,raw_data)
        # json_ = json.loads(raw_data)
        # producer.send(topic_name, json_["text"].encode('utf-8'))
        return True

# Twitter -> Raw Data (Streaming Sevice) -> Kafka Producer (Local Host - 9092) Json - Consumer (Spark Streaming client) -> RDD stuff as per
# api then -> Mongo db (Bson) -> Api (Mongo Query)
if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
