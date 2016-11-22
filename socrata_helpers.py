import arrow
import requests
import json

def FetchPublicData(resource):
    print('fetch public socrata data')
    
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)
    
    try:
        res = requests.get(url, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def UpsertData(creds, payload, resource):
    print('upsert open data ' + resource)
    
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)

    try:
        auth = (creds['user'], creds['password'])
        json_data = json.dumps(payload)
        res = requests.post(url, data=json_data, auth=auth, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e
    
    return res.json()



def PrepPubLog(date_time, event, socrata_response):
    print('prep publication log')

    if socrata_response['Errors']:
        response_message = socrata_response['message']

    else:
        response_message = ''

    return [ {
        'event': event,
        'timestamp': date_time.timestamp, 
        'date_time':  date_time.format('YYYY-MM-DD HH:mm:ss'),
        'errors': socrata_response['Errors'],
        'updated': socrata_response['Rows Updated'],
        'created': socrata_response['Rows Created'],
        'deleted': socrata_response['Rows Deleted'],
        'response_message': response_message
    } ]


