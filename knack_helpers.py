import json
import logging
import pdb
import requests



logger = logging.getLogger(__name__)



def update_record(record_dict, knack_object, id_key, creds):
    #  record object must have 'KNACK_ID' field
    print('update knack record')

    knack_id = record_dict[id_key]  #  extract knack ID and remove from update object
    del record_dict[id_key]

    update_url = 'https://api.knack.com/v1/objects/{}/records/{}'.format(knack_object, knack_id)

    headers = { 'x-knack-application-id': creds['app_id'], 'x-knack-rest-api-key': creds['api_key'], 'Content-type': 'application/json'}
    
    try:
        req = requests.put(update_url, headers=headers, json=record_dict)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()



def insert_record(record_dict, knack_object, id_key, creds):
    print('update knack record')
    
    insert_url = 'https://api.knack.com/v1/objects/{}/records'.format(knack_object)
    
    headers = { 'x-knack-application-id': creds['app_id'], 'x-knack-rest-api-key': creds['api_key'], 'Content-type': 'application/json'}

    try:
        req = requests.post(insert_url, headers=headers, json=record_dict)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()



def get_fields(knack_objects, creds):
    print('get knack field metadata')

    objects_url = 'https://api.knack.com/v1/objects/'

    fields = []
    raw_fields = {}

    headers = { 'x-knack-application-id': creds['app_id'], 'x-knack-rest-api-key': creds['api_key'] }

    try:
        req = requests.get(objects_url, headers=headers)

    except requests.exceptions.HTTPError as e:
        raise e

    data = req.json()['objects']  #  get all database objects

    for knack_object in data:  

        if knack_object["key"] in knack_objects:  #  for the objects we care about, get all the field metadata

            current_object = knack_object["key"]

            url = "{}{}/fields?rows_per_page=1000".format(objects_url, current_object)

            try:
                req = requests.get(url, headers=headers)

            except requests.exceptions.HTTPError as e:
                raise e

            fields = fields + req.json()['fields']

    for field in fields:

        if field['key'] + '_raw' not in raw_fields:

            raw_fields[field['key'] + '_raw'] = field  # raw fields only--that's the good stuff
 
    return raw_fields



def create_field_lookup(field_dict, **options): 
    #  use options['parse_raw'] to handle 'raw' field names
    #  its not ideal
    if not 'parse_raw' in options:
        options['parse_raw'] = False

    field_lookup = {}
    
    for field in field_dict:
        
        if options['parse_raw']:
            new_field = field.replace('_raw', '')

        field_lookup[field_dict[field]['label']] = new_field

    return field_lookup



def create_label_list(list_of_fields, **options): 

    field_labels = []
    
    for field in list_of_fields:

        field_labels.append(field['label'])

    return list( set(field_labels) )



def get_data(scene, view, creds):
    '''
    get knack data from a view configured in the application
    '''
    print('get knack data')
    
    table_url = 'https://api.knack.com/v1/pages/scene_{}/views/view_{}/records?rows_per_page=1000'.format( scene, view )

    current_page = 1

    headers = { 'x-knack-application-id': creds['app_id'], 'x-knack-rest-api-key': creds['api_key'] }
    
    params = {'page':current_page}

    try:
        req = requests.get(table_url, headers=headers, params=params)

    except requests.exceptions.HTTPError as e:
        raise e

    data = req.json()['records']

    if (req.json()['total_pages'] > 1):

        total_pages = req.json()['total_pages']

        while current_page < total_pages: #  page numbers start at 0

            current_page = current_page + 1

            params = {'page':current_page}

            try:
                req = requests.get(table_url, headers=headers, params=params)

                data = data + req.json()['records']
            
            except requests.exceptions.HTTPError as e:
                raise e

    print("retrieved {} records".format(len(data)))

    return data



def get_all_fields(knack_object, knack_params):
    #  object must have < 1000 fields
    #  return all field metadata for a given object
    print('get all knack field metadata for {}'.format(knack_object))

    objects_url = 'https://api.knack.com/v1/objects/'

    headers = { 'x-knack-application-id': knack_params['app_id'], 'x-knack-rest-api-key': knack_params['api_key'] }

    url = "{}{}/fields?rows_per_page=1000".format(objects_url, knack_object)

    try:
        req = requests.get(url, headers=headers)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()['fields']



def parse_data(data, field_list, **options):
    print('parse knack data')
    #  create a happy list of dicts from raw knack data
    #  data is a list of dicts from knack database
    #  option include_ids adds a field for the Knack DB IDs
    #  knack ID outfield is 'KNACK_ID' unless specified in option 'id_outfield'
    #  option require_location throws out records missing a 'LONGITUDE' field
    if 'include_ids' not in options:
        options['include_ids'] = False

    if 'require_locations' not in options:
        options['require_locations'] = False

    if 'convert_to_unix' not in options:
        options['convert_to_unix'] = False

    if 'raw_connections' not in options:
        options['raw_connections'] = False
    
    if 'id_outfield' not in options:
        options['id_outfield'] = 'KNACK_ID'

    parsed_data = []

    count = 0

    for record in data:
 
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

            if key in field_list:
                field_label = field_list[key]['label']

                field_type = field_list[key]['type']

            else:
                continue

            if field_type == 'address':  #  converts location field to lat/lon  

                new_record['LATITUDE'] = record[key]['latitude']

                new_record['LONGITUDE'] = record[key]['longitude']

            elif field_type == 'date':
                new_record[field_label] = record[key]['unix_timestamp']
                
                if options['convert_to_unix']:
                    new_record[field_label] = int( float(new_record[field_label]) / 1000 ) 
            
            elif field_type == 'date_time':
                new_record[field_label] = record[key]['unix_timestamp']

                if options['convert_to_unix']:
                    new_record[field_label] =  int( float(new_record[field_label]) / 1000 )

            elif field_type == 'connection':

                if options['raw_connections']:
                    new_record[field_label] = record[key]
                
                else:
                    new_record[field_label] = record[key][0]['identifier']

            else:
                new_record[field_label] = record[key]

        if options['include_ids']:
            id_outfield = options['id_outfield']
            
            new_record[id_outfield] = record['id']

        if options['require_locations']:
            
            if  'LONGITUDE' in new_record:
                parsed_data.append(new_record)

        else:
            parsed_data.append(new_record)

    return parsed_data



def get_object_data(knack_object, creds):
    '''
    fetch knack data directly from knack table ('object') 
    instead ofr from a page view
    '''
    print('get knack data')
    
    objects_url = 'https://api.knack.com/v1/objects/{}/records?rows_per_page=1000'.format( knack_object ) 

    current_page = 1

    headers = { 'x-knack-application-id': creds['app_id'], 'x-knack-rest-api-key': creds['api_key'] }
    
    params = {'page':current_page}

    try:
        req = requests.get(objects_url, headers=headers, params=params)

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

    return data