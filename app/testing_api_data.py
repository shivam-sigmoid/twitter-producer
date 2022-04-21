from random import randint

from geopy.geocoders import Nominatim


def get_country(loc):
    user_ag = 'user_me_{}'.format(randint(10000, 99999))
    geolocator = Nominatim(user_agent=user_ag)
    location = geolocator.geocode(loc)
    temp = location.address
    # print(temp)
    temp = temp.split(',')
    return temp[-1].lstrip()


print(get_country("gomti nagar"))
