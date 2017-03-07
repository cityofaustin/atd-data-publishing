import requests
import json
import pdb


def update_record(record_dict, knack_params):
    #  record object must have 'KNACK_ID' field
    print('update knack record')

    knack_id = record_dict['KNACK_ID']  #  extract knack ID and remove from update object
    del record_dict['KNACK_ID']

    update_url = 'https://api.knack.com/v1/objects/{}/records/{}'.format(knack_params['REFERENCE_OBJECTS'][0], knack_id)

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'], 'Content-type': 'application/json'}

    try:
        req = requests.put(update_url, headers=headers, json=record_dict)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()



def get_fields(knack_params):
    print('get knack field metadata')

    objects_url = 'https://api.knack.com/v1/objects/'

    fields = []
    filtered_fields = {}

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'] }

    try:
        req = requests.get(objects_url, headers=headers)

    except requests.exceptions.HTTPError as e:
        raise e
    
    data = req.json()['objects']  #  get all database objects

    for knack_object in data:  

        if knack_object["key"] in knack_params['REFERENCE_OBJECTS']:  #  for the objects we care about, get all the field metadata

            current_object = knack_object["key"]

            url = "{}{}/fields?rows_per_page=1000".format(objects_url, current_object)

            try:
                req = requests.get(url, headers=headers)

            except requests.exceptions.HTTPError as e:
                raise e

            fields = fields + req.json()['fields']

    for field in fields:  #  keep only the fields we want

        if field['label'] in knack_params['FIELD_NAMES']:

                filtered_fields[field['key'] + '_raw'] = field  #  we append raw here because we only want to access the raw fields
 
    return filtered_fields



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

    return field_labels



def get_data(knack_params):
    print('get knack data')
    
    table_url = 'https://api.knack.com/v1/pages/scene_{}/views/view_{}/records?rows_per_page=1000'.format( knack_params['SCENE'], knack_params['VIEW'])

    current_page = 1

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'] }
    
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

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'] }

    url = "{}{}/fields?rows_per_page=1000".format(objects_url, knack_object)

    try:
        req = requests.get(url, headers=headers)

    except requests.exceptions.HTTPError as e:
        raise e

    return req.json()['fields']



def parse_data(data, field_list, knack_params, **options):
    print('parse knack data')
    #  create a happy list of dicts from raw knack data
    #  data is a list of dicts from knack database
    #  only handles on location field
    #  option include_ids adds a 'knack_id' field 
    #  option require_location throws out dicts missing a 'LONGITUDE' field

    if 'include_ids' not in options:
        options['include_ids'] = False

    if 'require_locations' not in options:
        options['require_locations'] = False

    if 'convert_to_unix' not in options:
        options['convert_to_unix'] = False
    
    field_names = knack_params['FIELD_NAMES']

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
                continue  #  ignore fields not in in-field list

            if field_label in field_names:  #  inclue only fields in out-field list                

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
                    new_record[field_label] = record[key][0]['identifier']

                else:
                    new_record[field_label] = record[key]

        if options['include_ids']:
            new_record['KNACK_ID'] = record['id']

        if options['require_locations']:
            
            if  'LONGITUDE' in new_record:
                parsed_data.append(new_record)

        else:
            parsed_data.append(new_record)

    return parsed_data



def get_object_data(knack_object, knack_params):
    print('get knack data')
    
    objects_url = 'https://api.knack.com/v1/objects/{}/records?rows_per_page=1000'.format( knack_object ) 

    current_page = 1

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'] }
    
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