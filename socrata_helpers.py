import arrow
import requests
import json
import pdb


def get_public_data(resource):
    print('fetch public socrata data')
    
    url = 'https://data.austintexas.gov/resource/{}.json?$limit=10000'.format(resource)
    
    try:
        res = requests.get(url, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def get_private_data(creds, resource):
    print('fetch private socrata data')
    
    url = 'https://data.austintexas.gov/resource/{}.json?$limit=10000'.format(resource)
    
    auth = (creds['user'], creds['password'])

    try:
        res = requests.get(url, auth=auth, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def create_payload(detection_obj, prim_key):
    #  readies a socrata upsert from the results of change detection
    #  see https://dev.socrata.com/publishers/upsert.html
    now = arrow.now()
    payload = []
    payload = payload + detection_obj['new'] + detection_obj['change']

    for record in payload:
        record['processed_datetime']  = now.timestamp
        record['record_id'] = '{}_{}'.format(record[prim_key], str(now.timestamp))

    for record in detection_obj['delete']:
        payload.append( { prim_key : record[prim_key], ':deleted' : True } )

    return payload



def create_location_fields(list_of_dicts):
    print('create location fields')

    for record in list_of_dicts:
        if 'LATITUDE' in record and 'LONGITUDE' in record:
            record['LOCATION'] = '({},{})'.format(record['LATITUDE'], record['LONGITUDE'])

    return list_of_dicts



def strip_geocoding(list_of_dicts):
    print('strip geocoding field')

    for record in list_of_dicts:
        if 'location' in record:
            if 'needs_recoding' in record['location']:
                del record['location']['needs_recoding']

    return list_of_dicts



def upsert_data(creds, payload, resource):
    print('upsert open data ' + resource)
    
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)
    
    try:
        auth = (creds['user'], creds['password'])
        json_data = json.dumps(payload)
        res = requests.post(url, data=json_data, auth=auth, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e
    
    return res.json()


def replace_data(creds, resource, payload):
    print('replace resource ' + resource)

    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)
    
    try:
        auth = (creds['user'], creds['password'])
        json_data = json.dumps(payload)
        res = requests.put(url, data=json_data, auth=auth)

    except requests.exceptions.HTTPError as e:
        raise e
    
    return res.json()



def replace_non_data_file(creds, resource, filename, file, timeout=10):
    print('replace non data file')
    # see here https://github.com/xmun0x/sodapy

    auth = (creds['user'], creds['password'])

    uri = 'https://data.austintexas.gov/api/views/{}.txt'.format(resource)

    files = {'file': (filename, file)}

    params = {'id': resource, 'method': 'replaceBlob'}
    
    try:
        res = requests.post(uri, files=files, auth=auth, params=params, timeout=timeout)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def prep_pub_log(date_time, event, socrata_response):
    print('prep publication log')

    if 'message' not in socrata_response:
        socrata_response['message'] = ''

    if 'error' in socrata_response:        

        return [ {
            'event': event,
            'timestamp': date_time.timestamp, 
            'date_time':  date_time.format('YYYY-MM-DD HH:mm:ss'),
            'response_message': socrata_response['message']
        }]

    return [ {
        'event': event,
        'timestamp': date_time.timestamp, 
        'date_time':  date_time.format('YYYY-MM-DD HH:mm:ss'),
        'errors': socrata_response['Errors'],
        'updated': socrata_response['Rows Updated'],
        'created': socrata_response['Rows Created'],
        'deleted': socrata_response['Rows Deleted'],
        'response_message': socrata_response['message']
    } ]



def add_hist_fields(list_of_dicts):

    for record in list_of_dicts:
        
        record_retired_datetime = arrow.now()
        record['record_retired_datetime'] = record_retired_datetime.format('YYYY-MM-DD HH:mm:ss')

        if 'processed_datetime' in record:
            processed_datetime = arrow.get(record['processed_datetime'])
            processed_datetime = processed_datetime.replace(tzinfo = 'US/Central')  # teach a naive date about the world
        
        else:
            continue
        
        delta = record_retired_datetime - processed_datetime

        record['operation_state_duration'] = delta.seconds

    return list_of_dicts

