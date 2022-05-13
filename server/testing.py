from iso3166 import countries
from datetime import datetime
import requests


def get_data_api6():
    #for c in countries:
    URL = "https://open.who.int/2020-21/contributors/contributor"
    country = "China"
    PARAMS = {'name': country}
    raw_data = requests.get(url=URL, params=PARAMS)
    print(raw_data.text)


if __name__ == "__main__":
    get_data_api6()
    #update_week_diseaseSh()
