from iso3166 import countries
from datetime import datetime
import requests
import sys

sys.path.append("../")
from data.API_Links import Disease_SH_URL
# import pymongo

# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# col = db["disease_sh"]

from database import Database

db = Database()
col = db.create_db_connection("disease_sh")


def update_week_diseaseSh():
    col.update_many(
        {},
        [
            {"$set": {"week": {"$week": "$date"}}}
        ]
    )


def get_week_num(date):
    l1 = str(date).split()
    return datetime.date(l1[2], l1[1], l1[0]).isocalendar().week


def get_data_api7():
    for c in countries:
        # URL = "https://disease.sh/v3/covid-19/historical"
        URL = Disease_SH_URL
        lastdays = 60
        PARAMS = {'lastdays': lastdays}
        URL = URL + '/' + c.alpha3
        raw_data = requests.get(url=URL, params=PARAMS)
        data = raw_data.json()
        try:
            cases = data['timeline']['cases']
            recovered = data['timeline']['recovered']
            data_list = []
            for date, value in cases.items():
                flatten_data = dict()
                flatten_data['country'] = data['country']
                flatten_data['date'] = datetime.strptime(date, '%m/%d/%y')
                # flatten_data['week'] = get_week_num(flatten_data['date'])
                flatten_data['rank'] = value - recovered[date]
                data_list.append(flatten_data)
            col.insert_many(data_list)
        except:
            print(f'Data for the Country {c.name} is not available!')


if __name__ == "__main__":
    get_data_api7()
    update_week_diseaseSh()
