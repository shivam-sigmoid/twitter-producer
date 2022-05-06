from flask_pymongo import PyMongo
import flask
from flask import render_template
import bson
import json
import datetime
import dateutil.parser
import pandas as pd
from utility_functions import get_weather, get_country, most_common_words


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


@app.route("/")
def hello():
    return render_template("index.html")


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
def task_2_all():
    tweets = db.tweets.aggregate([
        {"$match": {"location": {"$exists": "true"}}},
        {"$group": {"_id": {"Country": "$location", "date": "$date"}, "tweets_per_day_per_Country": {"$sum": 1}}},
        {"$project": {"_id.date": 1, "_id.Country": 1, "tweets_per_day": 1, "tweets_per_day_per_Country": 1}},
        {"$sort": {"tweets_per_day_per_Country": -1}}
    ])
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        tweets_dict[i] = {k: v for k, v in tweet.items()}
        i += 1
    return flask.jsonify(tweets_dict)


@app.route("/task_2/<raw_date>")
def task_2(raw_date):
    print(raw_date)
    date = dateutil.parser.parse(raw_date)
    tweets = db.tweets.aggregate([
        {"$match": {"location": {"$exists": "true"}}},
        {"$match": {"date": datetime.datetime(date.year, date.month, date.day, 18, 30, 00)}},
        {"$group": {"_id": {"Country": "$location"}, "tweets_per_day_per_Country": {"$sum": 1}}},
        {"$sort": {"tweets_per_day_per_Country": -1}}
    ])
    tweets_dict = dict()
    i = 0
    for tweet in tweets:
        tweets_dict[i] = {k: v for k, v in tweet.items()}
        i += 1
    return flask.jsonify(tweets_dict)


@app.route("/task_3")
def task_3():
    tweets = db.tweets.find()
    return flask.jsonify(most_common_words(tweets))


@app.route("/task_4/<loc>")
def task_4(loc):
    # print(loc)
    loc = get_country(loc)
    query = {"location": str(loc)}
    tweets = db.tweets.find(query)
    return flask.jsonify(most_common_words(tweets))


@app.route("/task_5")
def task_5_all():
    infos = db.measures.find()
    data_list = list()
    for info in infos:
        data_list.append(info['country'])
        data_list.append(info['measures_taken'])
    return flask.jsonify(data_list)


@app.route("/task_5/<country>")
def task_5(country):
    cnt = get_country(country)
    # print(cnt)
    query = {"country": str(cnt)}
    infos = db.measures.find(query)
    data_list = list()
    data_list.append(cnt)
    for info in infos:
        data_list.append(info['measures_taken'])
    return flask.jsonify(data_list)


@app.route("/task_6")
def task_6():
    df = pd.read_excel("../data/Donations.xlsx", sheet_name="Overall")
    return render_template('task_6.html', column_names=df.columns.values, row_data=list(df.values.tolist()), zip=zip)


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


@app.route("/task_9/<country>")
def task_9(country):
    cnt = get_country(country)
    weather_details = get_weather(country)
    query = {"country": str(cnt)}
    infos = db.age_weather_data.find(query)
    data_list = list()
    # Append the Country Name
    data_list.append(cnt)
    # Append the Country's weather data
    data_list.append(weather_details)
    for info in infos:
        data_list.append(info['data'])
    return flask.jsonify(data_list)


if __name__ == "__main__":
    app.run(debug=True, port=5005)
