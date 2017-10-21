import json
import logging
import pdb

import arrow
import requests


class Soda(object):
    
    def __init__(
        self,
        resource=None,
        user=None,
        password=None,
        portal='data.austintexas.gov',
        record_limit=5000,
        timeout=10
    ):
        '''  
        Class to interact with Socrata-powered data portal via the SODA API as
        documented at https://dev.socrata.com/consumers/getting-started.html
        
        Parameters
        ----------
        '''
        if not resource:
            raise Excpetion('Resource identifier is required.')

        self.resource = resource
        self.portal = portal
        self.user = user
        self.password = password
        self.record_limit = int(record_limit)
        self.timeout = float(timeout)
        
        self.url = 'https://{}/resource/{}.json?$limit={}'.format(
            self.portal,
            self.resource,
            self.record_limit
        )
        
        self.url_metadata = 'https://{}/api/views/{}.json'.format(
            self.portal,
            self.resource
        )

        self.data = None
        self.metadata = None
        self.fieldnames = None

        if not (user and password):
            logging.warning('Username and password are required for private datasets.')
            self.data = self.get_public_data()

        else:
            self.data = self.get_private_data()

    def get_public_data(self):
        print('fetch public socrata data')
        res = requests.get(self.url, verify=False)
        self.data = res.json()
        return self.data

    def get_private_data(self):
        print('fetch private socrata data')
        
        auth = (
            self.user,
            self.password
        )
        
        res = requests.get(
            self.url,
            auth=auth,
            verify=False
        )

        return res.json()

    def get_metadata(self):
        print('Get metadata')
        
        auth = (
            self.user,
            self.password
        )
        
        res = requests.get(
            self.url_metadata,
            auth=auth,
            verify=False
        )
        
        self.metadata = res.json()
        self.get_fieldnames()
        self.get_date_fields()
        return self.metadata

    def get_fieldnames(self):
        self.fieldnames = [ col['fieldName'] for col in self.metadata['columns'] ]
        return self.fieldnames

    def get_date_fields(self):
        self.date_fields = [ field['fieldName'] for field in self.metadata['columns'] if 'date' in field['dataTypeName'] ] 
        return self.date_fields


def create_payload(detection_obj, prim_key):
    #  readies a socrata upsert from the results of change detection
    #  see https://dev.socrata.com/publishers/upsert.html
    now = arrow.now()
    payload = []
    payload = payload + detection_obj['new'] + detection_obj['change']

    for record in payload:
        record['processed_datetime'] = now.timestamp
        
        record['record_id'] = '{}_{}'.format(
            record[prim_key],
            str(now.timestamp)
        )

    for record in detection_obj['delete']:
        payload.append({
            prim_key : record[prim_key],
            ':deleted' : True }
        )

    return payload


def create_location_fields(
    dicts,
    lat_field='LOCATION_latitude',
    lon_field='LOCATION_longitude'
):
    
    for record in dicts:
        #  create location field if lat and lon are avaialble
        if lat_field in record and lon_field in record:
            
            record['location'] = '({},{})'.format(
                record[lat_field],
                record[lon_field]
            )

        else:
            #  otherwise create empty location field
            record['location'] = ''

    return dicts


def strip_geocoding(dicts):
    print('strip geocoding field')

    for record in dicts:
        if 'location' in record:
            if 'needs_recoding' in record['location']:
                del record['location']['needs_recoding']
            if 'human_address' in record['location']:
                del record['location']['human_address']

    return dicts


def upsert_data(creds, payload, resource):
    print('upsert open data ' + resource)
    
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)    
    auth = (creds['user'], creds['password'])
    json_data = json.dumps(payload)

    res = requests.post(
        url,
        data=json_data,
        auth=auth,
        verify=False
    )
    
    return res.json()


def replace_resource(creds, resource, payload):
    print('replace resource ' + resource)
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)    
    auth = (creds['user'], creds['password'])
    json_data = json.dumps(payload)
    res = requests.put(url, data=json_data, auth=auth)
    return res.json()


def replace_non_data_file(creds, resource, filename, file, timeout=10):
    print('replace non data file')
    # see here https://github.com/xmun0x/sodapy
    auth = (creds['user'], creds['password'])
    uri = 'https://data.austintexas.gov/api/views/{}.txt'.format(resource)
    files = {'file': (filename, file)}
    params = {'id': resource, 'method': 'replaceBlob'}
    
    res = requests.post(
        uri,
        files=files,
        auth=auth,
        params=params,
        timeout=timeout
    )

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


def add_hist_fields(dicts):

    for record in dicts:
        
        record_retired_datetime = arrow.now()
        
        record['record_retired_datetime'] = record_retired_datetime.format(
            'YYYY-MM-DD HH:mm:ss'
        )

        if 'processed_datetime' in record:
            processed_datetime = arrow.get(record['processed_datetime'])
            processed_datetime = processed_datetime.replace(tzinfo = 'US/Central')  # teach a naive date about the world
        
        else:
            continue
        
        delta = record_retired_datetime - processed_datetime

        record['operation_state_duration'] = delta.seconds

    return dicts

