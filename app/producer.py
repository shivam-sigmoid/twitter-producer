import tweepy

# API Secret Keys

consumer_key = "siIbnTn8GcBUsfySy0VmjyR7A"
consumer_secret = "PBH7bOVRrv7gEiksWhvczVdFYjiHLIogv3RxQHpfV65gfjI36F"
access_token = "1510924415642791938-DXjvrckvFFT05fiAXQ5oUDmmrqpN98"
access_token_secret = "VOuZDj8EMGbyfQx2Aen4h7FLAsuxSsZmp4Ry5WanCUni8"

# Kafka Producer

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])  # Same port as your Kafka server

topic_name = "test-topic"

# Live Data from Twitter Streaming

class TwitterStreamer():

    def stream_tweets(self):
        while True:
            stream = ListenerTS(consumer_key, consumer_secret, access_token, access_token_secret)
            stream.filter(track=["covid", "corona", "covid-19"], stall_warnings=True, languages=["en"])

# Listener on data

class ListenerTS(tweepy.Stream):
    def on_data(self, raw_data):
        producer.send(topic_name, raw_data)
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
