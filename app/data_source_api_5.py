import json
import requests as r
import pytz
import sys
sys.path.append("../")

from data.API_Links import Measures_Taken_URL
# import pymongo
from database import Database
db = Database()
col = db.create_db_connection("measures")

# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# col = db["measures"]
# response = r.get("http://covidsurvey.mit.edu:5000/query?country=all&signal=measures_taken")

response = r.get(Measures_Taken_URL)
json_data = json.loads(response.text)


data_list = []

for x,y in json_data.items():
    # print(x)
    # print(pytz.country_names[x])
    # print(y['measures_taken'])
    dic = dict()
    dic['country'] = pytz.country_names[x]
    dic['measures_taken'] = y['measures_taken']
    data_list.append(dic)

col.insert_many(data_list)



# task_3
# mongo query 3:
#
# db.tweets.aggregate([
#   { $project: { words: { $split: ["$full_text", " "] } } },
#   { $unwind: "$words" },
#   { $match : { words: { $nin: ["a", "I", "are", "is", "to", "the", "of", "and"]} } },
#   { $group: { _id: "$words" , total: { "$sum": 1 } } },
#   { $sort: { total : -1 } },
#   { $limit: 100 }
# ]);

# task_4

# db.tweets.aggregate([
# { $project: { location:"$location", words: { $split: ["$full_text", " "] } } },
# { $unwind: "$words" },
# { $match : { words: { $nin: ['',"a", "I", "are", "is", "to", "the", "of", "and", "in", "RT", "was","on" , "for"]} } },
# { $group: { _id: {location:"$location", tweet: "$words"},total: { "$sum": 1 } } },
# { $sort: { total : -1 } }
# ])
