#  it works.

import json
import pdb

import requests

import _setpath
from config.secrets import *


def query(method, url, data=None, auth=JOB_DB_API_TOKEN):
    
    headers = {
        'Content-Type' : 'text/csv',
        'Prefer' : 'return=representation'  # return entire record json in response
    }

    if method.upper() == 'SELECT':
        res = requests.get(url, headers=headers)

    elif method.upper() == 'INSERT':
        headers['Authorization'] = f'Bearer {auth}'
        res = requests.post(url, headers=headers, data=data)
    
    elif method.upper() == 'UPDATE':
        headers['Authorization'] = f'Bearer {auth}'
        #  require ID match to prevent unintentional batch update
        _id = data.pop(self.id_field)
        url = f'{url}?id=eq.{_id}'

        res = requests.patch(url, headers=headers, json=data)
    
    elif method.upper() == 'DELETE':
        headers['Authorization'] = f'Bearer {auth}'
        #  this will delete all rows that match query!
        res = requests.delete(url, headers=headers)

    else:
        raise Exception('Unknown method requested.')

    res.raise_for_status()
    return res.json()


keys = [
    'traffic_report_id',
    'published_date',
    'address',
    'issue_reported',
    'latitude',
    'longitude',
    'traffic_report_status',
    'traffic_report_status_date_time'
]

url = f'https://data.austintexas.gov/resource/r3af-2r8x.json?$select=traffic_report_id, published_date, address, issue_reported, location_latitude as latitude, location_longitude as longitude, traffic_report_status, traffic_report_status_date_time&$limit=100000'
db = 'http://transportation-data.austintexas.io/traffic_reports'

res = requests.get(url)
records = res.json()

for record in records:
    for key in keys:
        if key not in record:
            record[key] = None

headers = {
    'Content-Type' : 'application/json',
    'Authorization' : f'Bearer {JOB_DB_API_TOKEN}',
    'Prefer' : 'resolution=merge-duplicates' 
}

print('loading records')
res = requests.post(db, headers=headers, json=records)

pdb.set_trace()













