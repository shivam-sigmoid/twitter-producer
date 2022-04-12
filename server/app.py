from flask_pymongo import PyMongo
import flask
import bson
import json


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
        {"$group": {"_id": {"Country": "$location"}, "tweets_per_day_per_Country": {"$sum": 1}}},
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}}, "tweets_per_day": {"$sum": 1}}},
        {"$project": {"_id.date": 1, "_id.Country": 1, "tweets_per_day": 1, "tweets_per_day_per_Country": 1}}
    ])
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        print(tweet)
        tweets_dict[i] = {k: v for k, v in tweet.items()}
        i += 1
    # print(tweets_dict)
    return flask.jsonify(tweets_dict)


app.run(debug=True, port=5000)
