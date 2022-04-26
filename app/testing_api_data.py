from random import randint

from geopy.geocoders import Nominatim


def get_country(loc):
    user_ag = 'user_me_{}'.format(randint(10000, 99999))
    geolocator = Nominatim(user_agent=user_ag)
    location = geolocator.geocode(loc)
    print(location)
    address = location.address
    # print(address)
    address_split = address.split(',')
    country = address_split[-1].lstrip()
    return country


print(get_country("abcfg"))