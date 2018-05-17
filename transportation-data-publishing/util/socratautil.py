import pdb

import arrow
import requests

import _setpath
from util import datautil

    
class Soda(object):
    '''
    Class to query and publish open data via the Socrata Open Data API (SODA)

    ** Requires custom datautil module **
    '''
    def __init__(self,
                 auth=None,
                 fetch_metadata=False,
                 host='data.austintexas.gov',
                 records=None,
                 resource=None,
                 soql=None,
                 date_fields=None,
                 lat_field='locaiton_latitude',
                 lon_field='location_longitude',
                 location_field='location',
                 replace=False):
            
        self.auth = auth
        self.date_fields = date_fields
        self.host = host
        self.lat_field = lat_field
        self.lon_field = lon_field
        self.location_field = location_field
        self.records = records
        self.replace = replace
        self.soql = soql
        
        if not resource:
            raise Exception('Socrata resouce ID is required.')

        self.resource = resource

        self.data = None
        self.fieldnames = None
        self.metadata = None

        self.url = f'https://{self.host}/resource/{self.resource}.json'
        self.url_metadata = f'https://{self.host}/api/views/{self.resource}.json'

        if self.records:
            self._handle_records()

        else:
            self._query()

            if fetch_metadata:
                self._get_metadata()
                self._get_fieldnames()
                self._get_date_fields()


    def _handle_records(self):
        if self.date_fields:
            self.records = datautil.mills_to_unix(self.records, self.date_fields)

        self.records = datautil.lower_case_keys(self.records)
        
        if self.location_field:
            self.records = self._location_fields()

        self.res = self._upload()
        self._handle_response()
        return self.res


    def _location_fields(self):
        '''
        Create special socrata "location" field from x/y values.
        '''
        for record in self.records:
            
            try:
                #  create location field if lat and lon are avaialble
                if record[self.lat_field] and record[self.lon_field]:
                    record[self.location_field] = '({},{})'.format(
                        record[self.lat_field],
                        record[self.lon_field]
                    )

                else:
                    record[self.location_field] = ''

            except KeyError:
                #  do not add location field if lat/lon keys are missing
                pass

        return self.records


    def _upload(self):

        if self.replace:
            res = requests.put(
                self.url,
                json=self.records,
                auth=self.auth
            )
        
        else:
            res = requests.post(
                self.url,
                json=self.records,
                auth=self.auth
            )

        res.raise_for_status()
        return res.json()


    def _handle_response(self):
        '''
        Parse socrata API response
        '''
        if 'error' in self.res:        
            raise Exception(self.res)

        elif self.res.get('Errors'):
            raise Exception(self.res)

        return True


    def _query(self):
        '''
        Query a socrata resource. soql must be a dict formated as { $key : value }
        as defined in the SoQl spec, here: https://dev.socrata.com/docs/queries/
        '''            
        res = requests.get(
            self.url,
            params=self.soql
        )

        res.raise_for_status()
        self.data = res.json()
        return res.json()


    def _get_metadata(self):        
        res = requests.get(
            self.url_metadata,
            auth=self.auth
        )        
        self.metadata = res.json()

        return self.metadata

    def _get_fieldnames(self):
        self.fieldnames = [ col['fieldName'] for col in self.metadata['columns'] if '@computed_region' not in col['fieldName']]
        return self.fieldnames

    def _get_date_fields(self):
        self.date_fields = [ field['fieldName'] for field in self.metadata['columns'] if 'date' in field['dataTypeName'] ] 
        return self.date_fields


def prepare_deletes(records, primary_key):
    #  Format socrata payload for record deletes
    #  See: https://dev.socrata.com/docs/
    deletes = []

    for record in records:
        deletes.append({
            primary_key : record[primary_key],
            ':deleted' : True }
        )

    return deletes


def strip_geocoding(dicts):
    '''
    Remove unwanted metadata from socrata location field
    '''
    for record in dicts:
        
        try:            
            location = record.get('location')
            record['location'] = {'latitude': location['latitude'], 'longitutde' : location['longitutde']}

        except KeyError:
            continue

    return dicts













