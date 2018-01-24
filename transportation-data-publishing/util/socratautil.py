import json
import logging
import pdb

import arrow
import requests

#  update all scripts
#  update crontab sig_req_rank_pub to signal_requests
#  bcycle is broken
#  test everything
#  test new sig status email error handling

class Soda(object):
    
    def __init__(
        self,
        resource=None,
        user=None,
        password=None,
        portal='data.austintexas.gov',
        soql={'$limit' : 5000},
        timeout=10
    ):
        '''  
        Class to interact with Socrata-powered data portal via the SODA API as
        documented at https://dev.socrata.com/consumers/getting-started.html
        
        This is a simplified version of Socrata's sodapy 
        (https://github.com/xmunoz/sodapy) which has been tailored to our
        data publishing needs.

        Parameters
        ----------
        resource : string (required)
            Socrata resource identifier
        portal : string
            Domain name of Socrata-powered data portal
        user : string
            Login username. Required for private datasets.
        password: string
            Login password. Required for private datasets.
        soql: dic
            A dictionary of SoQL clauses, where each key/value is a parameter
            and value to be passed as a query string to the endpoint URL. As
            described here: https://dev.socrata.com/docs/queries/
            Default parameter is record limit of 5k records.
        timeout
            The request timeout time in seconds

        Returns a Soda instance with data stored in dictionary at self.data.
        Default record limit is 5k records.
        '''
        if not resource:
            raise Exception('Resource identifier is required.')

        self.resource = resource
        self.portal = portal
        self.user = user
        self.password = password
        self.soql = soql
        self.timeout = float(timeout)
        
        if self.user and self.password:
            self.auth = (self.user, self.password)
        else:
            self.auth = None

        self.url = 'https://{}/resource/{}.json'.format(
            self.portal,
            self.resource,
        )

        self.url_metadata = 'https://{}/api/views/{}.json'.format(
            self.portal,
            self.resource
        )

        self.data = None
        self.metadata = None
        self.fieldnames = None


    def get_data(self):
        params = self.soql
        
        res = requests.get(
            self.url,
            params=params,
            auth=self.auth
        )

        res.raise_for_status()
        self.data = res.json()
        return self.data


    def get_metadata(self):        
        res = requests.get(
            self.url_metadata,
            auth=self.auth
        )
        
        self.metadata = res.json()

        self._get_fieldnames()
        self._get_date_fields()
        return self.metadata


    def _get_fieldnames(self):
        self.fieldnames = [ col['fieldName'] for col in self.metadata['columns'] ]
        return self.fieldnames


    def _get_date_fields(self):
        self.date_fields = [ field['fieldName'] for field in self.metadata['columns'] if 'date' in field['dataTypeName'] ] 
        return self.date_fields



def create_payload(detection_obj, prim_key, add_timestamp=False, generate_ids=False):
    #  readies a socrata upsert from the results of change detection
    #  see https://dev.socrata.com/publishers/upsert.html
    payload = []
    payload = payload + detection_obj['new'] + detection_obj['change']

    for record in payload:
        if add_timestamp:
            now = arrow.now()
            record['processed_datetime'] = now.timestamp
        
        if generate_ids:
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
    lat_field='location_latitude',
    lon_field='location_longitude',
    location_field='location',
    ignore_missing=True
):
    '''
    Create special socrata "location" field from x/y values.
    '''
    for record in dicts:
        
        try:
            #  create location field if lat and lon are avaialble
            if record[lat_field] and record[lon_field]:
                record[location_field] = '({},{})'.format(
                    record[lat_field],
                    record[lon_field]
                )
            else:
                record[location_field] = ''

        except KeyError:
            #  do not add locaiton field if lat/lon keys are missing
            if ignore_missing:
                pass
            else:
                raise KeyError

    return dicts


def strip_geocoding(dicts):
    '''
    Remove unwanted metadata from socrata location field
    '''
    for record in dicts:
        if 'location' in record:
            if 'needs_recoding' in record['location']:
                del record['location']['needs_recoding']
            if 'human_address' in record['location']:
                del record['location']['human_address']
    return dicts


def upsert_data(creds, payload, resource):
    print('Upsert open data ' + resource)
    
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)    
    auth = (creds['user'], creds['password'])
    json_data = json.dumps(payload)

    res = requests.post(
        url,
        data=json_data,
        auth=auth
    )
    
    return res.json()


def replace_resource(creds, resource, payload):
    '''
    Replace the entire contents of a socrata dataset. Achieved by sending
    a PUT request.
    '''
    url = 'https://data.austintexas.gov/resource/{}.json'.format(resource)    
    auth = (creds['user'], creds['password'])
    json_data = json.dumps(payload)
    res = requests.put(url, data=json_data, auth=auth)
    return res.json()


def replace_non_data_file(creds, resource, filename, file, timeout=10):
    print('Replace non-data file.')
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


def pub_log_payload(
    id_,
    start,
    end,
    resource=None,
    dataset=None,
    created=0,
    updated=0,
    deleted=0,
    errors=0,
    message=None
    ):
    '''
    Get a publication log payload dict 
    '''
    return {
        'id' : id_,
        'dataset' : dataset,
        'resource' : resource,
        'created' : created,
        'updated' : updated,
        'deleted' : deleted,
        'errors' : errors,
        'message' : message,
        'start' : start,
        'end' : end,
        'duration' : end - start,
    }



def handle_response(res, log_dict):
    '''
    Parse socrata API response and assign publication log payload attributes
    '''
    if 'error' in res:        
        log_dict['errors'] = 1
        log_dict['message'] = res.get('message')
        return log_dict

    else:
        log_dict['errors'] = res.get('Errors', 0)
        log_dict['updated'] = res.get('Rows Updated', 0)
        log_dict['created'] = res.get('Rows Created', 0)
        log_dict['deleted'] = res.get('Rows Deleted', 0)
        log_dict['message'] = res.get('message')
        return log_dict




