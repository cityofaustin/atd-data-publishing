#  update Knack street segments with data from COA ArcGIS Online Feature Service
import argparse
import logging
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import agolutil
from util import datautil
from util import emailutil


def get_params(layer_config):

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


def handleFeatures(features, layer_config, location):
    handler = layer_config['handle_features']
    if handler == 'use_first' or len(features) == 1:
        #  use first feature in results and join feature data to location record
        feature = features[0]
        for field in feature['attributes'].keys():
            #  remove whitespace from janky Esri fields
            try:
                location[field] = str(feature['attributes'][field]).strip()
            except KeyError:
                continue

    elif handler == 'merge_all' and len(features) > 1:
        #  concatenate feature data from all retrieved features 
        #  and join to location record
        for feature in features:
            for field in feature['attributes'].keys():
                if field not in location:
                    location[field] = []
                    
                location[field].append(str(feature['attributes'][field]).strip())

    return location


def main(date_time):
    print('starting stuff now')

    try:
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

        keep_fields = [field for field in kn.fieldnames if field not in outfields]
        kn.data = datautil.reduce_to_keys(kn.data, keep_fields)
        total = len(kn.data)
        for location in kn.data:
            print('Processing {} of {}'.format(count, total))
            count += 1

            point = [ 
                location['LOCATION_longitude'],
                location['LOCATION_latitude']
            ]

            for layer in layers:
                layer['geometry'] = point

                params = get_params(layer)

                try:
                    res = agolutil.point_in_poly(
                        layer['service_name'],
                        layer['layer_id'],
                        params
                    )                    
                
                    
                    if len(res['features']) > 0:
                        location = handleFeatures(res['features'], layer, location)
                        continue

                    else:
                        #  no intersecting features found
                        #  set outfields to null to overwrite any existing data
                        for field in outfields:
                            if field in layer['outFields']:
                                location[field] = ''

                        logging.info(location)
                        continue

                except Exception as e:
                    unmatched_locations.append(location)
                    print("Unable to retrieve segment {}".format(location))
                    raise e
            
            location['UPDATE_PROCESSED'] = True
            location = datautil.reduce_to_keys([location], outfields + ['id'])
            location = datautil.replace_keys(location, kn.field_map)
            
            response_json = knackpy.update_record(
                location[0],
                obj,
                'id',
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
    now_s = now.format('YYYY_MM_DD')

    #  init logging 
    logfile = '{}/location_updater_{}.log'.format(LOG_DIRECTORY, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    #  config
    knack_creds = KNACK_CREDENTIALS
    obj = 'object_11'

    outfields = [
        'JURISDICTION_LABEL',
        'SIGNAL_ENG_AREA',
        'COUNCIL_DISTRICT',
        'UPDATE_PROCESSED'
    ]

    layers = [
        {
            'service_name' : 'BOUNDARIES_single_member_districts',
            'outFields' : 'COUNCIL_DISTRICT',
            'layer_id' : 0,
            'distance' : 100,
            'units' : 'esriSRUnit_Foot',
            #  how to handle query that returns multiple intersection features
            'handle_features' : 'merge_all'  
        },
        {
            'service_name' : 'BOUNDARIES_jurisdictions',
            'outFields' : 'JURISDICTION_LABEL',
            'layer_id' : 0,
            'handle_features' : 'use_first'
        },
        {
            'service_name' : 'ATD_signal_engineer_areas',
            'outFields' : 'SIGNAL_ENG_AREA',
            'layer_id' : 0,
            'handle_features' : 'use_first'
        }
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

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))




