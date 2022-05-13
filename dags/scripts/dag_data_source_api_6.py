import json
import requests as r

import pymongo

client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
db = client["twitter_db"]
col = db["donation"]

link = "https://covidfunding.eiu.com/api/funds"

data_list = []

response = r.get(link)
json_data = json.loads(response.text)

for key, value in json_data.items():

    for doc in value:
        # print(doc)
        try:
            data_list.append(dict(doc))
        except Exception:
            continue

col.insert_many(data_list)
