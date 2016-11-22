import requests



def GetFields(knack_params):
    
    print('generate object/field list')

    fields = []
    filtered_fields = {}

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'] }

    try:
        req = requests.get(knack_params['OBJECTS_URL'], headers=headers)

    except requests.exceptions.HTTPError as e:
        raise e
    
    data = req.json()['objects']  #  get all database objects

    for knack_object in data:  

        if knack_object["key"] in knack_params['REFERENCE_OBJECTS']:  #  for the objects we care about, get all the field metadata

            current_object = knack_object["key"]

            url = "{}{}/fields?rows_per_page=1000".format(knack_params['OBJECTS_URL'], current_object)

            try:
                req = requests.get(url, headers=headers)

            except requests.exceptions.HTTPError as e:
                raise e

            fields = fields + req.json()['fields']

    for field in fields:  #  keep only the fields we want

        if field['label'] in knack_params['FIELD_NAMES']:

                filtered_fields[field['key'] + '_raw'] = field  #  we append raw here because we only want to access the raw fields

    return filtered_fields



def GetData(knack_params):
    print('fetch data from query')

    url = knack_params['TABLE_URL']

    current_page = 1

    headers = { 'x-knack-application-id': knack_params['APPLICATION_ID'], 'x-knack-rest-api-key': knack_params['API_KEY'] }
    
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

    return data



def ParseData(data, field_list, knack_params):
    print('parse Knack data')
    
    in_fields = knack_params['FIELD_NAMES']

    out_fields = knack_params['OUT_FIELDS']

    parsed_data = []

    count = 0

    for record in data:

        count += 1
        
        new_record = {}
        
        for key in record:
            
            if not record[key]:
                continue

            if key in field_list:
                field_label = field_list[key]['label']

                field_type = field_list[key]['type']

            else:
                continue

            if field_label == 'GEOCODE':  #  world's hackiest solution
                field_label = 'LATITUDE'

            if field_label in out_fields:                
                new_record[field_label] = ''

                if field_type == 'address':

                    new_record['LATITUDE'] = record[key]['latitude']

                    new_record['LONGITUDE'] = record[key]['longitude']

                elif field_type == 'date':
                    new_record[field_label] = record[key]['date']
                
                elif field_type == 'date_time':
                    new_record[field_label] = record[key]['date']

                elif field_type == 'connection':
                    new_record[field_label] = record[key][0]['identifier']

                else:
                    new_record[field_label] = record[key]

        for field in out_fields:

            if field not in new_record:

                new_record[field] = ''

        if  new_record['LONGITUDE']:  #  include only records with geometry
            parsed_data.append(new_record)

    return parsed_data