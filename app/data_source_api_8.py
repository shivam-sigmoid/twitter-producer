################################################################################
# https://data.oecd.org/price/share-prices.htm
# https://data.oecd.org/gdp/quarterly-gdp.htm
# https://data.oecd.org/price/inflation-cpi.htm
# country
'''
/country/
--> yearly analysis data
country_name:
year:
gdp:
gdp_growth_rate:
cpi:
cpi_growth_rate:
shares:
share_growth_rate:

/country/year/
--> yearly analysis data
country_name:
year:
gdp:
cpi:
shares:

/country/year/month
--> monthly analysis data
country_name:
year:
month:
cpi:
shares:

/country/year/quater
--> quater analysis data
country_name:
year:
quater:
GDP:
cpi:
shares:

'''
import csv
import calendar
import pycountry

from database import Database
# import pymongo

# from geopy import Nominatim

mon_col = []
q_col = []
yr_col = []
country_by_alpha3 = {country.alpha_3: country.name for country in pycountry.countries}
# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# col_1 = db["monthly_api_8"]
# col_2 = db["quarterly_api_8"]
# col_3 = db["yearly_api_8"]


db = Database()
col_1 = db.create_db_connection("monthly_api_8")
col_2 = db.create_db_connection("quarterly_api_8")
col_3 = db.create_db_connection("yearly_api_8")


# locator = Nominatim(user_agent="myGeocoder")

def insert_monthly_data():
    with open('../data/CPI_Mon.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            d = dict()
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            try:
                alpha3 = row[0]
                ctry = country_by_alpha3[alpha3]
                d['country'] = ctry
                d['year'] = int(row[5].split('-')[0])
                month = int(row[5].split('-')[1])
                d['month_name'] = calendar.month_name[int(month)]
                d['cpi'] = float(row[6])
                # print(d)
                mon_col.append(d)
            except Exception as e:
                print(e)
    with open('../data/Share_Mon.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            alpha3 = row[0]
            ctry = country_by_alpha3[alpha3]
            year = int(row[5].split('-')[0])
            month = int(row[5].split('-')[1])
            month_name = calendar.month_name[int(month)]
            for d in mon_col:
                if d['country'] == ctry and d['year'] == year and d['month_name'] == month_name:
                    d['share'] = float(row[6])
        # for i in range(5):
        #     print(mon_col[i])
        col_1.insert_many(mon_col)


def insert_quarterly_data():
    with open('../data/CPI_Q.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            d = dict()
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            try:
                alpha3 = row[0]
                ctry = country_by_alpha3[alpha3]
                d['country'] = ctry
                d['year'] = int(row[5].split('-')[0])
                d['quarter'] = row[5].split('-')[1]
                d['cpi'] = float(row[6])
                # print(d)
                q_col.append(d)
            except Exception as e:
                print(e)
    with open('../data/Share_Q.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            alpha3 = row[0]
            ctry = country_by_alpha3[alpha3]
            year = int(row[5].split('-')[0])
            q = row[5].split('-')[1]
            for d in q_col:
                if d['country'] == ctry and d['year'] == year and d['quarter'] == q:
                    d['share'] = float(row[6])
    with open('../data/GDP_Q.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            alpha3 = row[0]
            ctry = country_by_alpha3[alpha3]
            year = int(row[5].split('-')[0])
            q = row[5].split('-')[1]
            for d in q_col:
                if d['country'] == ctry and d['year'] == year and d['quarter'] == q:
                    d['gdp'] = float(row[6])
        # for i in range(5):
        #     print(mon_col[i])
        col_2.insert_many(q_col)


def insert_yearly_data():
    with open('../data/CPI_Yr.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            d = dict()
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            try:
                alpha3 = row[0]
                ctry = country_by_alpha3[alpha3]
                d['country'] = ctry
                d['year'] = int(row[5])
                d['cpi'] = float(row[6])
                # print(d)
                yr_col.append(d)
            except Exception as e:
                print(e)
    with open('../data/Share_Yr.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            alpha3 = row[0]
            ctry = country_by_alpha3[alpha3]
            year = int(row[5])
            for d in yr_col:
                if d['country'] == ctry and d['year'] == year:
                    d['share'] = float(row[6])
    with open('../data/GDP_Yr.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[5] == "TIME":
                continue
            if "2016" in row[5]:
                continue
            if row[0] in ["G-20", "EU27_2020", "EA19", "OECD", "OECDE", "G-7"]:
                continue
            alpha3 = row[0]
            ctry = country_by_alpha3[alpha3]
            year = int(row[5])
            for d in yr_col:
                if d['country'] == ctry and d['year'] == year:
                    d['gdp'] = float(row[6])
        # for i in range(5):
        #     print(mon_col[i])
        col_3.insert_many(yr_col)


if __name__ == "__main__":
    insert_monthly_data()
    insert_quarterly_data()
    insert_yearly_data()
