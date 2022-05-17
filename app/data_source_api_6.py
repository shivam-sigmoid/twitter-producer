import json
import requests as r
import sys
sys.path.append("../")
from data.API_Links import covid_funding_url
# import pymongo
# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# col = db["donation"]
# link = "https://covidfunding.eiu.com/api/funds"
from database import Database
db = Database()
col = db.create_db_connection("donation")



data_list = []

response = r.get(covid_funding_url())
json_data = json.loads(response.text)

for key, value in json_data.items():

    for doc in value:
        # print(doc)
        try:
            data_list.append(dict(doc))
        except Exception:
            continue

col.insert_many(data_list)
