#  update Knack street segments with data from COA ArcGIS Online Feature Service
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import logging
import pdb
import arrow
import knack_helpers
import agol_helpers
import email_helpers
import data_helpers
import secrets

log_directory = secrets.LOG_DIRECTORY

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

#  init logging 
logfile = '{}/location_updater_{}.log'.format(log_directory, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

#  config
knack_creds = secrets.KNACK_CREDENTIALS
objects = ['object_11']
scene = '425'
view = '1201'

outfields = ['JURISDICTION_LABEL', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'GEOCODE', 'LATITUDE', 'LONGITUDE', 'UPDATE_PROCESSED']

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


def main(date_time):
    print('starting stuff now')

    try:       

        field_dict = knack_helpers.get_fields(objects, knack_creds)

        field_lookup = knack_helpers.create_field_lookup(field_dict, parse_raw=True)
        
        field_lookup = data_helpers.reduce_dicts([field_lookup], outfields)[0]  #  send field_lookup to reduce_dicts as a list
        
        field_lookup['KNACK_ID'] = 'KNACK_ID'  #  append to field lookup to avoid dropping when keys are replaced with db fieldnames

        knack_data = knack_helpers.get_data(scene, view, knack_creds)
        
        knack_data = knack_helpers.parse_data(knack_data, field_dict, include_ids=True, require_locations=True)

        payload = []
        update_response = []
        unmatched_locations = []

        count = 0

        for location in knack_data:
            
            count += 1
            print( 'featching data for record {} of {}'.format( count, len(knack_data) ) )

            point = [ location['LONGITUDE'], location['LATITUDE'] ]

            for layer in layers:

                try:
                    
                    intersect = agol_helpers.point_in_poly(layer['service_name'], layer['layer_id'], point, layer['outfields'])
                    
                    for field in layer['outfields']:
                        if field in intersect:
                            
                            if type(intersect[field]) == str:
                                #  remove whitespace from janky Esri fields
                                intersect[field] = intersect[field].strip()

                            location[field] = intersect[field]

                except Exception as e:
                    unmatched_locations.append(location)
                    print("Unable to retrieve segment {}".format(location))

            location['UPDATE_PROCESSED'] = True
            
            payload = data_helpers.replace_keys([location], field_lookup, delete_unmatched=True)
            
            response_json = knack_helpers.update_record(payload[0], objects[0], 'KNACK_ID', knack_creds)

            update_response.append(response_json)

        if (len(unmatched_locations) > 0):
            email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Location Point/Poly Match Failure', str(unmatched_locations))

        logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
        return update_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Location Update Failure', str(e))
        raise e


results = main(now)






