import requests
import json
from secrets import SOCRATA_CREDENTIALS


url = 'https://data.austintexas.gov/resource/utgi-umz5.json'

auth = (SOCRATA_CREDENTIALS['user'], SOCRATA_CREDENTIALS['password'])

json_data = [{
    "atd_intersection_id":"48",
    ":deleted": "true"
}]

res = requests.post(url, data=json_data, auth=auth)

print(res.json())
res.raise_for_status()














