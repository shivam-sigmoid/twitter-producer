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
from geopy import Nominatim

mon_col = []
q_col = []
yr_col = []
locator = Nominatim(user_agent="myGeocoder")
with open('data/CPI_Mon.csv', 'r') as file:
    reader = csv.reader(file)
    cnt = 0
    for row in reader:
        if row[5] == "TIME":
            continue
        if "2016" in row[5]:
            continue
        if row[0] in ["G-20", "EU27_2020"]:
            continue
        #location = locator.geocode(row[0])
        #country = str(location).split(', ')[3]
        year = int(row[5].split('-')[0])
        month = int(row[5].split('-')[1])
        month_name = calendar.month_name[int(month)]
        cpi = float(row[6])

        #print(year, cpi)


# with open('data/Share_Q.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)
#
# with open('data/Share_YR.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)
#
# with open('data/GDP_Q.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)
#
# with open('data/GDP_Yr.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)
#
# with open('data/CPI_Mon.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)
#
# with open('data/CPI_Q.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)
#
# with open('data/CPI_Yr.csv', 'r') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)