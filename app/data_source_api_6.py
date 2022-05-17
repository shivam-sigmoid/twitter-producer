import json
import requests as r
import sys
sys.path.append("../")
from data.API_Links import Covid_Funding_URL
# import pymongo
# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# col = db["donation"]
# link = "https://covidfunding.eiu.com/api/funds"
from database import Database
db = Database()
col = db.create_db_connection("donation")



data_list = []

response = r.get(Covid_Funding_URL)
json_data = json.loads(response.text)

for key, value in json_data.items():

    for doc in value:
        # print(doc)
        try:
            data_list.append(dict(doc))
        except Exception:
            continue

col.insert_many(data_list)
