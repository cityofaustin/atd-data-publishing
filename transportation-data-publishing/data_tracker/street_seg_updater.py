'''
update Knack street segments with data from 
COA ArcGIS Online Street Segment Feature Service
'''
import logging
import os
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import agolutil
from util import emailutil
from util import datautil


now = arrow.now()

script = os.path.basename(__file__).replace('.py', '.log')
logfile = f'{LOG_DIRECTORY}/{script}'
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

#  config
primay_key = 'SEGMENT_ID_NUMBER'
knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
ref_obj = ['object_7']
scene = 'scene_424'
view = 'view_1198'


def main(date_time):
    print('starting stuff now')

    try:       

        kn = knackpy.Knack(
                scene=scene,
                view=view,
                ref_obj=ref_obj,
                app_id=knack_creds['app_id'],
                api_key=knack_creds['api_key']
        )

        payload = []
        unmatched_segments = []
        
        if not kn.data:
            logging.info('No records to update.')
            return None

        for street_segment in kn.data:
            
            try:
                token = agolutil.get_token(AGOL_CREDENTIALS)
                features = agolutil.query_atx_street(street_segment[primay_key], token)

                if features.get('features'):
                    if len(features['features']) > 0:
                        segment_data = features['features'][0]['attributes']
                    else:
                        unmatched_segments.append(street_segment[primay_key])
                        continue
                
                else:
                    unmatched_segments.append(street_segment[primay_key])
                    continue

                segment_data['id'] = street_segment['id']
                segment_data['MODIFIED_BY'] = 'api-update'
                segment_data['CREATED_DATE'] = arrow.now().timestamp * 1000
                segment_data['UPDATE_PROCESSED'] = True
                payload.append(segment_data)
            
            except Exception as e:
                unmatched_segments.append(street_segment[primay_key])
                print("Unable to retrieve segment {}".format(street_segment[primay_key]))
                raise(e)

        payload = datautil.reduce_to_keys(payload, kn.fieldnames)
        payload = datautil.replace_keys(payload, kn.field_map)
        
        update_response = []
        count = 0

        for record in payload:
            count += 1
            print( 'updating record {} of {}'.format( count, len(payload) ) )

            #  remove whitespace from janky Esri attributes 
            for field in record:
                if type(record[field]) == str:
                    record[field] = record[field].strip()
            
            res = knackpy.record(
                record,
                obj_key=ref_obj[0],
                app_id= knack_creds['app_id'],
                api_key=knack_creds['api_key'],
                method='update',
            )

            update_response.append(res)

        if (len(unmatched_segments) > 0):
            logging.info( 'Unmatched Street Segments: {}'.format(unmatched_segments) )
            emailutil.send_email(ALERTS_DISTRIBUTION, 'Unmatched Street Segments', str(unmatched_segments), EMAIL['user'], EMAIL['password'])

        return update_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        emailutil.send_email(ALERTS_DISTRIBUTION, 'Street Segment Update Failure', str(e), EMAIL['user'], EMAIL['password'])
        raise e


results = main(now)
logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)



