from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="geoapiExercises")
ladd1 = "uk"
print("Location address:",ladd1)
location = geolocator.geocode(ladd1)
print("Street address, street name: ")
print(location.address)