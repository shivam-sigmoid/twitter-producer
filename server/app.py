import json
import bson
import flask
from flask import render_template
from flask_pymongo import PyMongo
import sys
sys.path.append("../")
from server.utility_functions import get_weather, get_country, most_common_words, get_task_6_data
from server.pipelines import get_pipeline_task_1, get_pipeline_task_2, get_pipeline_task_2_date_wise, \
    get_pipeline_task_7, \
    get_pipeline_task_7_week_wise
import logging


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
def home():
    return render_template("index.html")


# @app.route("/documentation")
# def documentation():
#     return render_template("documentation.html")


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
    try:
        tweets = db.tweets.aggregate(get_pipeline_task_1())
        tweets_dict = dict()
        i = 0
        for tweet in tweets:
            # print(tweet)
            tweets_dict[i] = {k: v for k, v in tweet.items()}
            i += 1
        # print(tweets_dict)
        logging.info("GET/200/Task 1")
        return flask.jsonify(tweets_dict)
    except Exception as e:
        logging.error(e)


@app.route("/task_2")
def task_2_all():
    try:
        tweets = db.tweets.aggregate(get_pipeline_task_2())
        tweets_dict = dict()
        i = 0
        for tweet in tweets:
            tweets_dict[i] = {k: v for k, v in tweet.items()}
            i += 1
        logging.info("GET/200/Task 2")
        return flask.jsonify(tweets_dict)
    except Exception as e:
        logging.error(e)


@app.route("/task_2/<raw_date>")
def task_2(raw_date):
    print(raw_date)
    try:
        tweets = db.tweets.aggregate(get_pipeline_task_2_date_wise(raw_date))
        tweets_dict = dict()
        i = 0
        for tweet in tweets:
            tweets_dict[i] = {k: v for k, v in tweet.items()}
            i += 1
        logging.info("GET/200/Task 2 Date Wise")
        return flask.jsonify(tweets_dict)
    except Exception as e:
        logging.error(e)


@app.route("/task_3")
def task_3():
    try:
        tweets = db.tweets.find()
        logging.info("GET/200/Task 3")
        return flask.jsonify(most_common_words(tweets))
    except Exception as e:
        logging.error(e)


@app.route("/task_4/<loc>")
def task_4(loc):
    # print(loc)
    try:
        loc = get_country(loc)
        query = {"location": str(loc)}
        tweets = db.tweets.find(query)
        logging.info("GET/200/Task 4 Location Wise")
        return flask.jsonify(most_common_words(tweets))
    except Exception as e:
        logging.error(e)


@app.route("/task_5")
def task_5_all():
    try:
        infos = db.measures.find()
        data_list = list()
        for info in infos:
            data_list.append(info['country'])
            data_list.append(info['measures_taken'])
        logging.info("GET/200/Task 5")
        return flask.jsonify(data_list)
    except Exception as e:
        logging.error(e)


@app.route("/task_5/<country>")
def task_5(country):
    try:
        cnt = get_country(country)
        # print(cnt)
        query = {"country": str(cnt)}
        infos = db.measures.find(query)
        data_list = list()
        data_list.append(cnt)
        for info in infos:
            data_list.append(info['measures_taken'])
        logging.info("GET/200/Task 5 Country wise")
        return flask.jsonify(data_list)
    except Exception as e:
        logging.error(e)


@app.route("/task_6")
def task_6():
    try:
        data_lst = get_task_6_data()
        response_data = dict()
        i = 1
        for data in data_lst:
            response_data[i] = {k: v for k, v in data.items()}
            i += 1
        logging.info("GET/200/Task 6")
        return flask.jsonify(response_data)
    except Exception as e:
        logging.error(e)


@app.route("/task_7/<int:week_num>")
def task_7(week_num):
    try:
        rankings = db.disease_sh.aggregate(get_pipeline_task_7_week_wise(week_num))
        rankings_dict = dict()
        i = 1
        for ranking in rankings:
            rankings_dict[i] = {k: v for k, v in ranking.items()}
            i += 1
        logging.info("GET/200/Task 7 Week Wise")
        return flask.jsonify(rankings_dict)
    except Exception as e:
        logging.error(e)


@app.route("/task_7")
def task_7_all():
    try:
        rankings = db.disease_sh.aggregate(get_pipeline_task_7())
        rankings_dict = dict()
        i = 1
        for ranking in rankings:
            rankings_dict[i] = {k: v for k, v in ranking.items()}
            i += 1
        logging.info("GET/200/Task 7")
        return flask.jsonify(rankings_dict)
    except Exception as e:
        logging.error(e)


@app.route("/task_9/<country>")
def task_9(country):
    try:
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
        logging.info("GET/200/Task 9 Country wise")
        return flask.jsonify(data_list)
    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    logging.basicConfig(filename='../logs/system.log',
                        format='%(asctime)s:%(levelname)s:%(message)s',
                        level=logging.DEBUG)
    # app.config['LOG_FILE'] = '../logs/system.log'
    app.run(debug=True, port=5005)
