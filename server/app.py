from flask_pymongo import PyMongo
import flask
import bson
import json
import nltk
import re
import itertools
import collections
from nltk.corpus import stopwords
from random import randint
from geopy.geocoders import Nominatim


def get_country(loc):
    user_ag = 'user_me_{}'.format(randint(10000, 99999))
    geolocator = Nominatim(user_agent=user_ag)
    location = geolocator.geocode(loc,language='en')
    if location is None:
        return loc
    address = location.address
    address_split = address.split(',')
    country = address_split[-1].lstrip()
    return country


def object_id_from_int(n):
    s = str(n)
    s = '0' * (24 - len(s)) + s
    return bson.ObjectId(s)


def int_from_object_id(obj):
    return int(str(obj))


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


app = flask.Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/twitter_db"
mongodb_client = PyMongo(app)
db = mongodb_client.db


# Location need to be uniformed as per country basis


@app.route("/get_tweets")
def get_tweets():
    tweets = db.tweets.find()
    print(type(tweets))
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        tweets_dict[i] = {k: v for k, v in tweet.items() if k != '_id'}
        i += 1
    return flask.jsonify(tweets_dict)


@app.route("/get_tweet/<int:tweet_id>")
def get_tweet(tweet_id):
    query = {"id": str(tweet_id)}
    tweets = db.tweets.find(query)
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        tweets_dict[i] = {k: v for k, v in tweet.items() if k != '_id'}
        i += 1
    return flask.jsonify(tweets_dict)


@app.route("/get_tweets_geo_enabled")
def get_tweets_with_geo():
    query = {"geo": {"$exists": "true"}}
    tweets = db.tweets.find(query)
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        # print(tweet)
        tweets_dict[i] = {k: v for k, v in tweet.items() if k != '_id'}
        i += 1
    print("count: ", i)
    return flask.jsonify(tweets_dict)


@app.route("/task_1")
def task_1():
    tweets = db.tweets.aggregate([
        {"$match": {"location": {"$exists": "true"}}},
        {"$group": {"_id": {"Country": "$location"}, "Total_Tweets_Per_Country": {"$sum": 1}}},
        {"$project": {"_id.Country": 1, "Total_Tweets_Per_Country": 1}},
        {"$sort": {"Total_Tweets_Per_Country": -1}}
    ])
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        # print(tweet)
        tweets_dict[i] = {k: v for k, v in tweet.items()}
        i += 1
    # print(tweets_dict)
    return flask.jsonify(tweets_dict)


@app.route("/task_2")
def task_2():
    tweets = db.tweets.aggregate([
        {"$match": {"location": {"$exists": "true"}}},
        {"$group": {"_id": {"Country": "$location", "date": "$date"}, "tweets_per_day_per_Country": {"$sum": 1}}},
        # {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}}, "tweets_per_day": {"$sum":
        # 1}}},
        {"$project": {"_id.date": 1, "_id.Country": 1, "tweets_per_day": 1, "tweets_per_day_per_Country": 1}},
        {"$sort": {"tweets_per_day_per_Country": -1}}
    ])
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        # print(tweet)
        tweets_dict[i] = {k: v for k, v in tweet.items()}
        i += 1
    # print(tweets_dict)
    return flask.jsonify(tweets_dict)


@app.route("/task_3")
def task_3():
    tweets = db.tweets.find()
    all_tweets = []
    for tweet in tweets:
        all_tweets.append(tweet['full_text'])

    def remove_url(txt):
        return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())

    all_tweets_no_urls = [remove_url(tweet) for tweet in all_tweets]
    words_in_tweet = [tweet.lower().split() for tweet in all_tweets_no_urls]
    all_words_no_urls = list(itertools.chain(*words_in_tweet))
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))
    tweets_nsw = []
    for w in all_words_no_urls:
        if w not in stop_words:
            tweets_nsw.append(w)

    tweets_nsw_nc = []
    collection_words = ['covid', 'covid19']
    for w in tweets_nsw:
        if w not in collection_words:
            tweets_nsw_nc.append(w)

    counts_nsw = collections.Counter(tweets_nsw_nc)
    return flask.jsonify(counts_nsw.most_common(100))


@app.route("/task_4/<loc>")
def task_4(loc):
    # print(loc)
    loc = get_country(loc)
    query = {"location": str(loc)}
    tweets = db.tweets.find(query)
    all_tweets = []
    for tweet in tweets:
        all_tweets.append(tweet['full_text'])

    def remove_url(txt):
        return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())

    all_tweets_no_urls = [remove_url(tweet) for tweet in all_tweets]
    words_in_tweet = [tweet.lower().split() for tweet in all_tweets_no_urls]
    all_words_no_urls = list(itertools.chain(*words_in_tweet))
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))
    tweets_nsw = []
    for w in all_words_no_urls:
        if w not in stop_words:
            tweets_nsw.append(w)

    tweets_nsw_nc = []
    collection_words = ['covid', 'covid19']
    for w in tweets_nsw:
        if w not in collection_words:
            tweets_nsw_nc.append(w)

    counts_nsw = collections.Counter(tweets_nsw_nc)
    return flask.jsonify(counts_nsw.most_common(100))


@app.route("/task_5/<country>")
def task_5(country):
    cnt = get_country(country)
    # print(cnt)
    query = {"country": str(cnt)}
    infos = db.measures.find(query)
    data_list = []
    data_list.append(cnt)
    for info in infos:
        data_list.append(info['measures_taken'])
    return flask.jsonify(data_list)


@app.route("/task_7/<int:week_num>")
def task_7(week_num):
    rankings = db.disease_sh.aggregate([
        {"$match": {"week": week_num}},
        {"$group": {"_id": {"Country": "$country"}, "rank": {"$sum": "$rank"}}},
        {"$sort": {"rank": -1}}
    ])
    rankings_dict = dict()
    i = 1
    for ranking in rankings:
        rankings_dict[i] = {k: v for k, v in ranking.items()}
        i += 1
    return flask.jsonify(rankings_dict)


@app.route("/task_7")
def task_7_all():
    rankings = db.disease_sh.aggregate([
        {"$group": {"_id": {"week": "$week", "Country": "$country"}, "rank": {"$sum": "$rank"}}},
        {"$sort": {"rank": -1}}
    ])
    rankings_dict = dict()
    i = 1
    for ranking in rankings:
        rankings_dict[i] = {k: v for k, v in ranking.items()}
        i += 1
    return flask.jsonify(rankings_dict)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
