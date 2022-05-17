# tweepy is an easy to use python library for accessing the Twitter API
from tweepy import OAuthHandler #tweepy supports OAuthHandler authentication
from tweepy import Stream #tweepy Stream establishes a streaming session and routes messages to StreamListner instance
from kafka import KafkaProducer #KafkaProducer module is imported from the Kafka library to write or publish events to Kafka
import configparser #configparser is a python class which implements a basic configuration language for python program
import json #Python has a built in packages called json, which can be used to work with JSON data

config = configparser.ConfigParser() #ConfigParser ia a python class which allows to write Python programs which cna be customized by end users easily
config.read('config.ini') #config.ini is a configuration file that consists of a text-based content with a structure and syntax comprising key-value pairs

consumer_key = config['twitter']['api_key']
consumer_secret = config['twitter']['api_key_secret']

access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

producer = KafkaProducer(bootstrap_servers='localhost:9092') #It is the URL of one of the Kafka brokers which we give to fetch the initial metadata about our Kafka cluster
topic_name = "covid_topic" #Kafka topics are the categories used to organise messages

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
            #tweets from twitter are filtered on the basis of these track
            stream.filter(track=["covid", "corona", "covid-19"], stall_warnings=True,
                          languages=["en"])

# Stream allows filtering and sampling of realtime Tweets using Twitter API
class ListenerTS(Stream):
    def on_data(self, raw_data):
        decoded = json.loads(raw_data)
        if not decoded['text'].startswith('RT'): #startwith() method return True if a string starts with specified prefix(string)
            json_str = json.dumps(decoded).encode('utf-8') #json.dumps() converts a Python object into a json string and encode() method encodes the string, using the specified encoding, default is UTF-8
            print(json_str)
            producer.send('covid_topic', value=json_str) #we have consumer listening to us, now generated messages that are published to Kafka and thereby consumed by our consumer created
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
