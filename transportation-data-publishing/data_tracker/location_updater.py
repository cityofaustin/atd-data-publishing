'''
Update Data Tracker location records with council district, engineer area,
and jurisdiction attributes from from COA ArcGIS Online feature services
'''
import argparse
import logging
import os
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import agolutil
from util import datautil
from util import emailutil


#  config
knack_creds = KNACK_CREDENTIALS
obj = 'object_11'

field_maps =  {
    #  service name
    'EXTERNAL_cmta_stops_new' : {  
        'fields' : {
            #  AGOL Field : Knack Field
            'ID' : 'BUS_STOPS',
        },
    },
}


def format_stringify_list(input_list):
    '''
    Function to format features when merging multiple feature attributes
    '''
    return ', '.join(str(l) for l in input_list)


'''
layer config for interacting with ArcGIS Online
see: http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#//02r3000000p1000000
'''
layers = [
    {   
        'service_name' : 'BOUNDARIES_single_member_districts',
        'outFields' : 'COUNCIL_DISTRICT',
        'updateFields' : ['COUNCIL_DISTRICT'], #
        'layer_id' : 0,
        'distance' : 33,  #  !!! this unit is interpreted as meters due to Esri bug !!!
        'units' : 'esriSRUnit_Foot', #  !!! this unit is interpreted as meters due to Esri bug !!!
        #  how to handle query that returns multiple intersection features
        'handle_features' : 'merge_all'  
    },
    {
        'service_name' : 'BOUNDARIES_jurisdictions',
        #  will attempt secondary service if no results at primary 
        'service_name_secondary' : 'BOUNDARIES_jurisdictions_planning',
        'outFields' : 'JURISDICTION_LABEL',
        'updateFields' : ['JURISDICTION_LABEL'],
        'layer_id' : 0,
        'handle_features' : 'use_first'
    },
    {
        'service_name' : 'ATD_signal_engineer_areas',
        'outFields' : 'SIGNAL_ENG_AREA',
        'updateFields' : ['SIGNAL_ENG_AREA'],
        'layer_id' : 0,
        'handle_features' : 'use_first'
    },
    {   
        'service_name' : 'EXTERNAL_cmta_stops_new',
        'outFields' : 'ID',
        'updateFields' : ['BUS_STOPS'],
        'layer_id' : 0,
        'distance' : 107,  #  !!! this unit is interpreted as meters due to Esri bug !!!
        'units' : 'esriSRUnit_Foot',  #  !!! this unit is interpreted as meters due to Esri bug !!!
        'handle_features' : 'merge_all',
        'apply_format' : format_stringify_list
    },
]

filters = {
    #  filter for records where
    #  UPDATE_PROCESSED field is No
    'match': 'and',
    'rules': [
        {
           'field':'field_1357',
           'operator':'is',
           'value':'No'
        }
    ]
}


def map_fields(record, field_map):
    '''
    Replace field names according to field map. Used to replace ArcGIS Online
    reference feature service field names with database field names.
    '''
    new_record = {}

    for field in record.keys():
        outfield = field_map['fields'].get(field)

        if outfield:
            new_record[outfield] = record[field]
        else:
            new_record[field] = record[field]

    return new_record


def get_params(layer_config):
    '''base params for AGOL query request'''
    params = {
        'f' : 'json',
        'outFields'  : '*',
        'geometry': None,
        'geomtryType' : 'esriGeometryPoint',
        'returnGeometry' : False,
        'spatialRel' :'esriSpatialRelIntersects',
        'inSR' : 4326,
        'geometryType' : 'esriGeometryPoint',
        'distance' : None,
        'units' : None
    }

    for param in layer_config:
        if param in params:
            params[param] = layer_config[param]

    return params

        

def join_features_to_record(features, layer_config, record):
    ''''
    Join feature attributes from ArcGIS Online query to location record
    
    Parameters
    ----------
    features : list (required)
        The 'features' array from an Esri query response object.
        See see: http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#//02r3000000p1000000
    layer_config : dict (required)
        The layer configuration dict that was provided to the ArcGIS Online query 
        and returned the providded features.
    record : dict (required)
        The source database record whose geomtetry intersects with
        the provided features

    Returns
    -------
    record (dict)
        The updated record object with location attributes attached
    '''
    handler = layer_config['handle_features']

    if handler == 'use_first' or len(features) == 1:
        #  use first feature in results and join feature data to location record
        feature = features[0]

        for field in feature['attributes'].keys():
            #  remove whitespace from janky Esri fields
            record[field] = str(feature['attributes'][field]).strip()

    elif handler == 'merge_all' and len(features) > 1:
        #  concatenate feature attributes from each feature and join to record
        for feature in features:
            for field in feature['attributes'].keys():
                if field not in record:
                    record[field] = []
                    
                record[field].append(str(feature['attributes'][field]).strip())

        if layer_config.get('apply_format'):
            #  apply special formatting function to attribute array
            for field in feature['attributes'].keys():
                input_val = record[field]
                record[field] = layer_config['apply_format'](input_val)

    return record


def cli_args():
    parser = argparse.ArgumentParser(
        prog='knack_data_pub.py',
        description='Publish Knack data to Socrata and ArcGIS Online'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        help='Name of the knack application that will be accessed'
    )

    args = parser.parse_args()
    
    return(args)


def main(date_time):
    print('starting stuff now')

    try:
        #  knack database fields that will be updated
        #  payload is reduced to these fields
        update_fields = [field for layer in layers for field in layer['updateFields']]

        kn = knackpy.Knack(
            obj=obj,
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key'],
            filters=filters,
            timeout=30
        )

        update_response = []
        unmatched_locations = []
        count = 0

        if not kn.data:
            logging.info('No new records to process')
            return None

        '''
        remove "update fields" from record. these are re-appended via
        spatial lookup
        '''  
        keep_fields = [field for field in kn.fieldnames if field not in update_fields]
        kn.data = datautil.reduce_to_keys(kn.data, keep_fields)
        
        for location in kn.data:
            count += 1

            point = [ 
                location['LOCATION_longitude'],
                location['LOCATION_latitude']
            ]

            for layer in layers:
                layer['geometry'] = point
                field_map = field_maps.get(layer['service_name'])
                params = get_params(layer)

                try:
                    res = agolutil.point_in_poly(
                        layer['service_name'],
                        layer['layer_id'],
                        params
                    )                    
                                
                    if res.get('features'):
                        location = join_features_to_record(
                            res['features'],
                            layer,
                            location
                        )
                        
                        if field_map:
                            location = map_fields(location, field_map)
                            
                        continue

                    if 'service_name_secondary' in layer:
                        '''
                        look for features at secondary service, if specified
                        '''
                        res = agolutil.point_in_poly(
                            layer['service_name_secondary'],
                            layer['layer_id'],
                            params
                        )

                        if len(res['features']) > 0:
                            location = join_features_to_record(
                                res['features'],
                                layer,
                                location
                            )

                            continue

                    #  no intersecting features found
                    for field in layer['updateFields']:
                        '''
                        set corresponding fields on location record to null to
                        overwrite any existing data
                        '''
                        location[field] = ''

                    continue

                except Exception as e:
                    unmatched_locations.append(location)
                    print("Unable to retrieve segment {}".format(location))
                    raise e
            
            location['UPDATE_PROCESSED'] = True
            location = datautil.reduce_to_keys([location], update_fields + ['id', 'UPDATE_PROCESSED'])
            location = datautil.replace_keys(location, kn.field_map)
            
            response_json = knackpy.update_record(
                location[0],
                obj,
                KNACK_CREDENTIALS[app_name]['app_id'],
                KNACK_CREDENTIALS[app_name]['api_key']
            )

            update_response.append(response_json)

        if (len(unmatched_locations) > 0):
            emailutil.send_email(
                ALERTS_DISTRIBUTION,
                'Location Point/Poly Match Failure',
                str(unmatched_locations), EMAIL['user'],
                EMAIL['password']
            )

        logging.info('{} records updated'.format(count))
        return update_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'Location Update Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e

if __name__ == '__main__':
    args = cli_args()
    app_name = args.app_name

    now = arrow.now()
    
    #  init logging 
    script = os.path.basename(__file__).replace('.py', '.log')
    logfile = f'{LOG_DIRECTORY}/{script}'
    logging.basicConfig(filename=logfile, level=logging.INFO)

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
