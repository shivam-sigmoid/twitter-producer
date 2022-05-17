import requests as r
import json
import pytz
import sys

sys.path.append("../")

from data.API_Links import Community_Risk_Index_URL
# import pymongo
#
# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# col = db["age_weather_data"]
# query = "http://covidsurvey.mit.edu:5000/query?gender=all&signal=community_risk_index"

from database import Database

db = Database()
col = db.create_db_connection("age_weather_data")

query = Community_Risk_Index_URL

# country=all&age=20-30
# could be one of all,20-30,31-40,41-50,51-60,61-70, or 71-80, default: all
# AF,AO,AR,AU,AZ,BD,BO,BR,CA,CI,CL,CM,CO,DE,DZ,EC,EE,EG,ES,FR,GB,GE,GH,GT,HN,ID,IN,IQ,IT,JM,JP,KE,KH,KR,KZ,\
# LK,MA,MM,MN,MX,MY,MZ,NG,NL,NP,PE,PH,PK,PL,PT,RO,SD,SG,SN,TH,TR,TT,TW,TZ,UA,UG,AE,US,UY,VE,VN,ZA

countries_list = ['AF', 'AO', 'AR', 'AU', 'AZ', 'BD', 'BO', 'BR', 'CA', 'CI', 'CL', 'CM', 'CO', 'DE', 'DZ', 'EC', 'EE',
                  'EG', 'ES', 'FR', 'GB', 'GE', 'GH', 'GT', 'HN', 'ID', 'IN', 'IQ', 'IT', 'JM', 'JP', 'KE', 'KH', 'KR',
                  'KZ', 'LK', 'MA', 'MM', 'MN', 'MX', 'MY', 'MZ', 'NG', 'NL', 'NP', 'PE', 'PH', 'PK', 'PL', 'PT', 'RO',
                  'SD', 'SG', 'SN', 'TH', 'TR', 'TT', 'TW', 'TZ', 'UA', 'UG', 'AE', 'US', 'UY', 'VE', 'VN', 'ZA']

age_group_list = ['20-30', '31-40', '41-50', '51-60', '61-70', '71-80']

data_list = list()
for country in countries_list:
    query_first = query + "&country=" + country
    lis = dict()
    dic = dict()
    for age in age_group_list:
        query_second = query_first + "&age=" + age
        response = r.get(query_second)
        lis[age] = json.loads(response.text)

    dic["country"] = pytz.country_names[country]
    dic["data"] = lis
    data_list.append(dic)

col.insert_many(data_list)
