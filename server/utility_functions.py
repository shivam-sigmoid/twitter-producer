from pyowm import OWM
import configparser
from random import randint
from geopy.geocoders import Nominatim
import nltk
import re
import itertools
import collections
from nltk.corpus import stopwords
import pandas as pd
import logging
import ssl


class Utils:
    # Function for getting the country
    # with the local, global area names
    @staticmethod
    def get_country(loc):
        try:
            user_ag = 'user_me_{}'.format(randint(10000, 99999))
            geolocator = Nominatim(user_agent=user_ag)
            location = geolocator.geocode(loc, language='en')
            if location is None:
                return loc
            address = location.address
            address_split = address.split(',')
            country = address_split[-1].lstrip()
            return country
        except Exception as e:
            logging.error(e)

    # Function to get the weather details of certain location
    @staticmethod
    def get_weather(cnt):
        try:
            config = configparser.ConfigParser()
            config.read('weather_config.ini')
            WEATHER_API_KEY = config['weather']['api_key']
            owm = OWM(WEATHER_API_KEY)
            mgr = owm.weather_manager()
            observation = mgr.weather_at_place(str(cnt))
            w = observation.weather
            weather_list = dict()
            # print(dir(w))
            weather_list["location"] = cnt.title()
            weather_list["temperature"] = w.temperature('celsius')
            weather_list["wind_speed"] = w.wind()
            weather_list["description"] = w.detailed_status
            weather_list["clouds"] = w.clouds
            weather_list["humidity"] = w.humidity
            return weather_list
        except Exception as e:
            logging.error(e)

    @staticmethod
    def most_common_words(tweets):
        try:
            all_tweets = []
            for tweet in tweets:
                all_tweets.append(tweet['full_text'])

            def remove_url(txt):
                return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())

            all_tweets_no_urls = [remove_url(tweet) for tweet in all_tweets]
            words_in_tweet = [tweet.lower().split() for tweet in all_tweets_no_urls]
            all_words_no_urls = list(itertools.chain(*words_in_tweet))
            try:
                _create_unverified_https_context = ssl._create_unverified_context
            except AttributeError:
                pass
            else:
                ssl._create_default_https_context = _create_unverified_https_context
            nltk.download('stopwords')
            stop_words = set(stopwords.words('english'))
            tweets_nsw = []
            for w in all_words_no_urls:
                if w not in stop_words:
                    tweets_nsw.append(w)

            tweets_nsw_nc = []
            collection_words = ['covid', 'covid19', 'donate', 'amp', 'get', 'us']
            for w in tweets_nsw:
                if w not in collection_words:
                    tweets_nsw_nc.append(w)

            counts_nsw = collections.Counter(tweets_nsw_nc)
            most_common = counts_nsw.most_common(100)
            return most_common
        except Exception as e:
            logging.error(e)

    @staticmethod
    def get_task_6_data():
        try:
            df = pd.read_excel("../data/Donations.xlsx", sheet_name="Overall")
            df_rows = df.values.tolist()
            df_cols = df.columns.values
            dict_lst = []
            for row in df_rows:
                d = dict()
                d[df_cols[0]] = row[0]
                d[df_cols[1]] = row[1]
                d[df_cols[2]] = '$' + str(round(row[2], 2)) + " Million"
                dict_lst.append(d)
            return dict_lst
        except Exception as e:
            logging.error(e)
