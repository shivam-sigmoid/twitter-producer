import json

import requests as r

response = r.get("http://covidsurvey.mit.edu:5000/query?country=all&signal=measures_taken")

json_data = json.loads(response.text)

print(json_data)
# print(json_data['AE']['measures_taken'])

# for k in json_data['AE']['measures_taken']:
#     print(k)

