from random import randint
from geopy.geocoders import Nominatim
import pymongo


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["twitter_db"]


def get_country(loc):
    if 'usa' in loc.lower():
        return 'United States'
    elif 'england' in loc.lower():
        return 'United Kingdom'
    elif 'london' in loc.lower():
        return 'United Kingdom'
    elif 'scotland' in loc.lower():
        return 'United Kingdom'
    elif 'india' in loc.lower():
        return 'India'
    elif 'mumbai' in loc.lower():
        return 'India'
    elif 'bangalore' in loc.lower():
        return 'India'
    # user_ag = 'user_me_{}'.format(randint(10000, 99999))
    # geolocator = Nominatim(user_agent=user_ag)
    # location = geolocator.geocode(loc)
    # if location is None:
    #     return loc
    # address = location.address
    # # print(address)
    # address_split = address.split(',')
    # country = address_split[-1].lstrip()
    # return country
    return loc


def update_tweets_loc():
    tweets = db.tweets.find()
    countries = dict()
    for tweet in tweets:
        id = tweet['_id']
        if 'location' in tweet.keys():
            location = tweet['location']
            if location in countries.keys():
                country = countries[location]
            else:
                country = get_country(location)
                countries[location] = country
            print('Before: ', location)
            db.tweets.update_one(
                {"_id": id},
                [
                    {"$set": {'location': country}}
                ]
            )
            temp = db.tweets.find(
                {"_id": id}
            )
            # for t in temp:
            #     print(t['location'])


if __name__ == "__main__":
    update_tweets_loc()
