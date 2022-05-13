from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import configparser
import json

# config = configparser.ConfigParser()
# config.read('config.ini')
#
# consumer_key = config['twitter']['api_key']
# consumer_secret = config['twitter']['api_key_secret']
#
# access_token = config['twitter']['access_token']
# access_token_secret = config['twitter']['access_token_secret']

consumer_key = "siIbnTn8GcBUsfySy0VmjyR7A"
consumer_secret = "PBH7bOVRrv7gEiksWhvczVdFYjiHLIogv3RxQHpfV65gfjI36F"
access_token = "1510924415642791938-DXjvrckvFFT05fiAXQ5oUDmmrqpN98"
access_token_secret = "VOuZDj8EMGbyfQx2Aen4h7FLAsuxSsZmp4Ry5WanCUni8"

producer = KafkaProducer(bootstrap_servers='host.docker.internal:9093')
topic_name = "covid_topic"
# print(producer)
print(producer.config)


# pkill -9 -f app/producer_twitter_data.py


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
            stream = ListenerTS(consumer_key, consumer_secret, access_token, access_token_secret)
            stream.filter(track=["covid", "corona", "covid-19"], stall_warnings=True,
                          languages=["en"])


class ListenerTS(Stream):
    def on_data(self, raw_data):
        decoded = json.loads(raw_data)
        if not decoded['text'].startswith('RT'):
            json_str = json.dumps(decoded).encode('utf-8')
            print(json_str)
            producer.send('covid_topic', value=json_str)
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
