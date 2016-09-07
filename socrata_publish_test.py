import requests
import json
from secrets import SOCRATA_CREDENTIALS


url = 'https://data.austintexas.gov/resource/5zpr-dehc.json'

auth = (SOCRATA_CREDENTIALS['user'], SOCRATA_CREDENTIALS['password'])

json_data = {
    "intid":"48",
    ":deleted": True
}

json_data = json.dumps(json_data)
res = requests.post(url, data=json_data, auth=auth)

print(res.json())
res.raise_for_status()














