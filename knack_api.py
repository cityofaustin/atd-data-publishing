import json
import logging
import pdb
import requests


class Knack(object):

    def __init__(self, app_id=None, api_key=None, timeout=10):
        '''  
        Class to interact with Knack application via the API as
        documented at https://www.knack.com/developer-documentation/
        
        Parameters
        ----------
        app_id : string (required)
            Knack application ID string
        api_key : string (optional | default : None )
            Knack application key. Required for accessing private views.
        timeout : numeric (optional | default: 10)
            number of seconds before http request timeout
        '''
        self.app_id = app_id
        self.api_key = api_key
        
        if not app_id:
            raise Exception('app_id is required.')

        if not api_key:
            logging.warning("API key is required to access private views.")

        self.timeout = float(timeout)


    def get_fields(self, objects, rows_per_page=1000):
        '''
        Get field data from Knack objects
        
        Parameters
        ----------
        rows_per_page : int (optional | default: 1000 | max: 1000)
            The number of rows to return per page (i.e. API call). 

        Returns
        -------
        fields : list
            A list of dictionaries where each dict is a Knack field 
        '''
        print('get field data')
        objects_url = 'https://api.knack.com/v1/objects/'

        fields = []
        raw_fields = {}

        headers = { 'x-knack-application-id': self.app_id, 'x-knack-rest-api-key': self.api_key }

        try:
            req = requests.get(objects_url, headers=headers)

        except requests.exceptions.HTTPError as e:
            raise e

        data = req.json()['objects']  #  get all database objects

        for obj in data:  
            
            if obj['key'] in objects:  #  get field metadata for specified objects

                current_object = obj['key']
                url = '{}{}/fields?rows_per_page=1000'.format(objects_url, current_object)

                try:
                    req = requests.get(url, headers=headers)

                except requests.exceptions.HTTPError as e:
                    raise e

                fields = fields + req.json()['fields']

        for field in fields:
            if field['key'] + '_raw' not in raw_fields:
                '''
                not all Knack fields have a 'raw' object
                so create one here
                '''
                raw_fields[field['key'] + '_raw'] = field  # 
        
        self.fields = raw_fields
        return self.fields

    def create_field_map(self, parse_raw=False): 
        field_map = {}
        
        for field in self.fields:
            if parse_raw:
                new_field = field.replace('_raw', '')

            field_map[self.fields[field]['label']] = new_field

        self.field_map = field_map
        return self.field_map

    def parse_data(self, include_ids=False, require_locations=False, convert_to_unix=False, raw_connections=False, id_outfield='KNACK_ID'):
        print('parse knack data')
        #  create a happy list of dicts from raw knack data
        #  data is a list of dicts from knack database
        #  option include_ids adds a field for the Knack DB IDs
        #  knack ID outfield is 'KNACK_ID' unless specified in option 'id_outfield'
        #  option require_location throws out records missing a 'LONGITUDE' field

        parsed_data = []

        count = 0

        for record in self.data:
     
            count += 1

            new_record = {}  #  parsed record goes here
            
            for key in record:  

                if type(record[key]) is str:
                    
                    record[key] = record[key].strip()

                    if record[key] == '':
                        continue  #  ignore empty fields

                if type(record[key]) is list:
                    if not record[key]:
                        continue  #  ignore empty fields

                if key in self.fields:
                    field_label = self.fields[key]['label']

                    field_type = self.fields[key]['type']

                else:
                    continue

                if field_type == 'address':  #  converts location field to lat/lon  

                    new_record['LATITUDE'] = record[key]['latitude']

                    new_record['LONGITUDE'] = record[key]['longitude']

                elif field_type == 'date':
                    new_record[field_label] = record[key]['unix_timestamp']
                    
                    if convert_to_unix:
                        new_record[field_label] = int( float(new_record[field_label]) / 1000 ) 
                
                elif field_type == 'date_time':
                    new_record[field_label] = record[key]['unix_timestamp']

                    if convert_to_unix:
                        new_record[field_label] =  int( float(new_record[field_label]) / 1000 )

                elif field_type == 'connection':

                    if raw_connections:
                        new_record[field_label] = record[key]
                    
                    else:
                        new_record[field_label] = record[key][0]['identifier']

                else:
                    new_record[field_label] = record[key]

            if include_ids:
                id_outfield = id_outfield
                
                new_record[id_outfield] = record['id']

            if require_locations:
                
                if  'LONGITUDE' in new_record:
                    parsed_data.append(new_record)

            else:
                parsed_data.append(new_record)

        self.data_parsed = parsed_data

        return self.data_parsed

class View(Knack):
    def __init__(self, scene, view, field_obj=None, app_id=None, api_key=None, timeout=10):
        Knack.__init__(self, app_id, api_key, timeout)
        
        '''
        Class to fetch data from a Knack application view via the API as
        documented at https://www.knack.com/developer-documentation/
        
        Parameters
        ----------
        scene : string (required)
            Knack scene identifer in format "scene_xx"
        view : string (required)
            Knack view identifier in format "view_xx"
        app_id : string (required)
            Knack application ID string
        api_key : string (optional | default : None )
            Knack application key. Required for accessing private views.
        fied_objects : list (optional | default : None)
            List of knack objects which are referefeced by the instance's view.
            Format "object_xx". Required for retrieving field data
        timeout : numeric (optional | default: 10)
            number of seconds before http request timeout
        '''
        self.scene = scene
        self.view = view
        self.field_obj = field_obj
        #  get field data and send to self.fields
        self.get_fields(self.field_obj)
        #  create field map and send to self.field_map
        self.create_field_map(parse_raw=True)
        #  get data and send to self.data
        self.data_from_view()

        if self.data and self.field_map:
            #  parse data and send to self.data_parsed
            self.parse_data(include_ids=True)

    def data_from_view(self, rows_per_page=1000):
        '''
        Get data from Knack view
        
        Parameters
        ----------
        rows_per_page : int (optional | default: 1000 | max: 1000)
            The number of rows to return per page (i.e. API call). 

        Returns
        -------
        data : list
            A list of dictionaries where each dict is a Knack record
        '''
        print("Get data from view {}".format(self.view))
        url = 'https://api.knack.com/v1/pages/{}/views/{}/records?rows_per_page={}'.format( self.scene, self.view, rows_per_page)
        current_page = 1
        headers = { 'x-knack-application-id': self.app_id, 'x-knack-rest-api-key': self.api_key }
        params = {'page':current_page}

        try:
            req = requests.get(url, headers=headers, params=params)

        except requests.exceptions.HTTPError as e:
            raise e

        data = req.json()['records']

        if (req.json()['total_pages'] > 1):
            total_pages = req.json()['total_pages']

            while current_page < total_pages: #  page numbers start at 0
                current_page = current_page + 1
                params = {'page':current_page}

                try:
                    req = requests.get(url, headers=headers, params=params)
                    data = data + req.json()['records']
                
                except requests.exceptions.HTTPError as e:
                    raise e

        print("retrieved {} records".format(len(data)))
        
        self.data = data
        return self.data


class Obj(Knack):
    '''
    Class to fetch data from a Knack object via the API as
    documented at https://www.knack.com/developer-documentation/
    
    Parameters
    ----------
    obj : string (required)
        Knack object identifer in format "object_xx"
    app_id : string (required)
        Knack application ID string
    api_key : string (optional | default : None )
        Knack application key. Required for accessing private views.
    timeout : numeric (optional | default: 10)
        number of seconds before http request timeout
    '''
    def __init__(self, obj, app_id=None, api_key=None, timeout=10):
        Knack.__init__(self, app_id, api_key, timeout)       

        self.obj = obj
        #  get field data and send to self.fields
        self.get_fields([self.obj])
        #  create field map and send to self.field_map
        self.create_field_map(parse_raw=True)
        #  get data and send to self.data
        self.data_from_obj()

        self.data_parsed = None
        if self.data and self.field_map:
            #  parse data and send to self.data_parsed
            self.parse_data(include_ids=True)

    def data_from_obj(self, rows_per_page=1000):
        print('hey');   
        '''
        fetch knack data directly from knack table ('object') 
        instead ofr from a page view
        '''
        print('get data from knack object')
        
        url = 'https://api.knack.com/v1/objects/{}/records?rows_per_page=1000'.format( self.obj ) 

        current_page = 1
        
        headers = { 'x-knack-application-id': self.app_id, 'x-knack-rest-api-key': self.api_key }
        
        params = {'page':current_page}

        try:
            req = requests.get(url, headers=headers, params=params)

        except requests.exceptions.HTTPError as e:
            raise e

        data = req.json()['records']

        if (req.json()['total_pages'] > 1):

            total_pages = req.json()['total_pages']

            while current_page < total_pages: #  page numbers start at 0

                current_page = current_page + 1

                params = {'page':current_page}

                try:
                    req = requests.get(objects_url, headers=headers, params=params)

                    data = data + req.json()['records']
                
                except requests.exceptions.HTTPError as e:
                    raise e

        print("retrieved {} records".format(len(data)))

        self.data = data
        return self.data




def update_record(record_dict, knack_object, id_key, app_id, api_key):
    #  record object must have 'KNACK_ID' field
    print('update knack record')

    knack_id = record_dict[id_key]  #  extract knack ID and remove from update object

    del record_dict[id_key]

    update_url = 'https://api.knack.com/v1/objects/{}/records/{}'.format(knack_object, knack_id)

    headers = { 'x-knack-application-id': app_id, 'x-knack-rest-api-key': api_key, 'Content-type': 'application/json'}
    
    try:
        req = requests.put(update_url, headers=headers, json=record_dict)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()
    

def insert_record(record_dict, knack_object, app_id, api_key):
    print('update knack record')
    
    insert_url = 'https://api.knack.com/v1/objects/{}/records'.format(knack_object)
    
    headers = { 'x-knack-application-id': app_id, 'x-knack-rest-api-key': api_key, 'Content-type': 'application/json'}

    try:
        req = requests.post(insert_url, headers=headers, json=record_dict)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()
