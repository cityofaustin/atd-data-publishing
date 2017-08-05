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
        kn = knackpy.Knack(
            obj=obj,
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key'],
            filters=filters
        )

        update_response = []
        unmatched_locations = []

        count = 0

        if not kn.data:
            logging.info('No new records to process')
            return None

        for location in kn.data:
            
            count += 1

            point = [ 
                location['LOCATION_longitude'],
                location['LOCATION_latitude']
            ]

            for layer in layers:
                try:
                    intersect = agolutil.point_in_poly(
                        layer['service_name'],
                        layer['layer_id'],
                        point,
                        layer['outfields']
                    )
                    
                    for field in layer['outfields']:
                        if field in intersect:
                            try:
                                #  remove whitespace from janky Esri fields
                                intersect[field] = intersect[field].strip()
                            except AttributeError:
                                pass

                            location[field] = intersect[field]

                except Exception as e:
                    unmatched_locations.append(location)
                    print("Unable to retrieve segment {}".format(location))

            location['UPDATE_PROCESSED'] = True
            location = datautil.reduce_to_keys([location], outfields)
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
        'UPDATE_PROCESSED',
        'id'
    ]

    layers = [
        {
            'service_name' : 'BOUNDARIES_single_member_districts',
            'outfields' : ['COUNCIL_DISTRICT'],
            'layer_id' : 0
        },
        {
            'service_name' : 'BOUNDARIES_jurisdictions',
            'outfields' : ['JURISDICTION_LABEL'],
            'layer_id' : 0
        },
        {
            'service_name' : 'ATD_signal_engineer_areas',
            'outfields' : ['SIGNAL_ENG_AREA'],
            'layer_id' : 0
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




