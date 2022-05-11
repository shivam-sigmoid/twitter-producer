from random import randint
# from geopy.geocoders import Nominatim
import pymongo


client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
db = client["twitter_db"]


def get_country(loc):
    if 'Suomi / Finland' in loc:
        return 'Finland'
    if 'Los Angeles' in loc:
        return 'United States'
    elif "\u0423\u043a\u0440\u0430\u0457\u043d\u0430" in loc:
        return 'Ukraine'
    elif 'UK' in loc:
        return 'United Kingdom'
    elif 'Melbourne' in loc:
        return 'Australia'
    elif 'Washington' in loc:
        return 'United States'
    elif 'Toronto' in loc:
        return 'Canada'
    elif 'New Delhi' in loc:
        return 'India'
    elif 'New York' in loc:
        return 'United States'
    elif 'Sydney' in loc:
        return 'Australia'
    elif 'Perth' in loc:
        return 'Australia'
    elif 'San Francisco' in loc:
        return 'United States'
    elif 'usa' in loc.lower():
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
