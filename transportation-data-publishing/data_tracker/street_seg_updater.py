'''
Update Knack street segments with data from 
COA ArcGIS Online Street Segment Feature Service
'''
import os
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *

from tdutils import agolutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil


def main():

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
        logger.info('No records to update.')
        return 0

    for street_segment in kn.data:
        
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
        segment_data['UPDATE_PROCESSED'] = True
        payload.append(segment_data)
    
    payload = datautil.reduce_to_keys(payload, kn.fieldnames)
    payload = datautil.replace_keys(payload, kn.field_map)
    
    update_response = []
    count = 1

    for record in payload:
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

        count += 1

        update_response.append(res)

    if (len(unmatched_segments) > 0):
        error_text = 'Unmatched street segments: {}'.format(', '.join( str(x) for x in unmatched_segments))
        logger.info(error_text)
        raise Exception(error_text)

    return count



if __name__ == '__main__':
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'

    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    primay_key = 'SEGMENT_ID_NUMBER'
    knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
    ref_obj = ['object_7']
    scene = 'scene_424'
    view = 'view_1198'

    try:
        
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)
        
        job.start()

        results = main()

        job.result('success', records_processed=results)

        logger.info('END AT {}'.format( arrow.now() ))
    
    except Exception as e:
        logger.error( str(e) )

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'Street Segment Update Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password'])

        job.result('error', message=str(e))
        raise e










