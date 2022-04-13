import tweepy
# from tweepy.streaming import StreamListener
from tweepy import OAuthHandler

# from tweepy import Stream
# from kafka import KafkaProducer
# import json

bearer_token = "AAAAAAAAAAAAAAAAAAAAAHl2bAEAAAAAJkTLa5NrJbPKg1Z3WE0DqAw6TRM%3DsKXH8eXWBsMyeHod08PHeki6c783x1BPo2YvtZHE2RHiYAp8In"
consumer_key = "siIbnTn8GcBUsfySy0VmjyR7A"
consumer_secret = "PBH7bOVRrv7gEiksWhvczVdFYjiHLIogv3RxQHpfV65gfjI36F"
access_token = "1510924415642791938-DXjvrckvFFT05fiAXQ5oUDmmrqpN98"
access_token_secret = "VOuZDj8EMGbyfQx2Aen4h7FLAsuxSsZmp4Ry5WanCUni8"

# auth = tweepy.OAuth2AppHandler(consumer_key, consumer_secret)


# Fill the X's with the credentials obtained by
# following the above mentioned procedure.


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

# from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers='localhost:9092')  # Same port as your Kafka server

# topic_name = "test-topic"

queryTopic = "(precaution OR precautions OR covid-19 OR covid OR corona) (from:WHO) lang:en"
for tweet in api.search_tweets(q=queryTopic, result_type="mixed", count=100, tweet_mode='extended'):
    # print(type(tweet))
    # print(dict(tweet))
    # print(tweet)
    if 'retweeted_status' in tweet._json:
        print(tweet._json['retweeted_status']['full_text'])
    else:
        print(tweet.full_text)
    print("============================================")
    # print(f"{tweet.user.name}:{tweet.text}")
    # strs = str(tweet.text)
    # producer.send(topic_name, json.dumps(tweet.text).encode('utf-8'))
    # producer.send(topic_name,str.encode(strs))
    # producer.send(topic_name,b"Hello-World")

# while True:
#     producer.send(topic_name,b'Hello-world')


# client = tweepy.Client(bearer_token=bearer_token)
#
# # Replace with your own search query
# query = 'covid-19'
#
# tweets = client.search_recent_tweets(query=query, tweet_fields=['context_annotations', 'created_at'], max_results=100)
#
# for tweet in tweets.data:
#     print(tweet.text)
#     if len(tweet.context_annotations) > 0:
#         print(tweet.context_annotations)


# class StdOutListener(tweepy.Stream):
#     def on_data(self, data):
#         json_ = json.loads(data)
#         producer.send("basic", json_["text"].encode('utf-8'))
#         return True
#
#     def on_error(self, status):
#         print(status)
#
#
# producer = KafkaProducer(bootstrap_servers=['hostname:9092'],api_version=(0,11,5))
#
# stream = StdOutListener(consumer_key,consumer_secret,access_token,access_token_secret)
# stream.filter(track=["covid19", "corona virus"])
