import arrow
import requests
import json


def FetchPublicData(resource):
    print('fetch public socrata data')
    
    url = 'https://data.austintexas.gov/resource/{}.json?$limit=10000'.format(resource)
    
    try:
        res = requests.get(url, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def FetchPrivateData(creds, resource):
    print('fetch private socrata data')
    
    url = 'https://data.austintexas.gov/resource/{}.json?$limit=10000'.format(resource)
    
    auth = (creds['user'], creds['password'])

    try:
        res = requests.get(url, auth=auth, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def CreatePayload(detection_obj, prim_key):
    #  readies a socrata upsert from the results of change detection
    #  see https://dev.socrata.com/publishers/upsert.html
    payload = []
    payload = payload + detection_obj['new'] + detection_obj['change']

    for record in detection_obj['delete']:
        payload.append( { prim_key : record[prim_key], ':deleted' : True } )

    return payload



def CreateLocationFields(data):
    print('create location fields')

    for record in data:
        if 'LATITUDE' in record and 'LONGITUDE' in record:
            record['LOCATION'] = '({},{})'.format(record['LATITUDE'], record['LONGITUDE'])

    return data



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


def ConvertToUnix(data):
    for record in data:
        for key in record:
            if '_DATE' in key:
                d = arrow.get(record[key], 'YYYY-MM-DDTHH:mm:ss')
                record[key] = str(d.timestamp)

    return data
    

















