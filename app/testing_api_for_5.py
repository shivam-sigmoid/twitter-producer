import json

import pytz

import requests as r

print(pytz.country_names['AE'])
response = r.get("http://covidsurvey.mit.edu:5000/query?country=all&signal=measures_taken")

json_data = json.loads(response.text)

print(json_data)
# print(json_data['AE']['measures_taken'])

# for k in json_data['AE']['measures_taken']:
#     print(k)

# task_3
# mongo query 3:
#
# db.tweets.aggregate([
#   { $project: { words: { $split: ["$text", " "] } } },
#   { $unwind: "$words" },
#   { $match : { words: { $nin: ["a", "I", "are", "is", "to", "the", "of", "and"]} } },
#   { $group: { _id: "$words" , total: { "$sum": 1 } } },
#   { $sort: { total : -1 } },
#   { $limit: 100 }
# ]);

