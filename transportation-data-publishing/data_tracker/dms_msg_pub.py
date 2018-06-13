'''
Extract DMS message from KITS database and upload to Data Tracker (Knack).
'''
import os
import pdb
import traceback
import sys

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import kitsutil
from tdutils import logutil

    
def main():
    
    kits_query =  '''
        SELECT DMSID as KITS_ID
        ,Multistring as DMS_MESSAGE
        ,LastUpdated as MESSAGE_TIME
        FROM [KITS].[DMS_RealtimeData]
        '''

    kits_data = kitsutil.data_as_dict(
        KITS_CREDENTIALS,
        kits_query
    )
    
    for record in kits_data:
        new_date = arrow.get(record['MESSAGE_TIME'])
        record['MESSAGE_TIME'] = new_date.timestamp * 1000

    kn = knackpy.Knack(
            scene=CONFIG['scene'],
            view=CONFIG['view'],
            ref_obj=CONFIG['ref_obj'],
            app_id=KNACK_CREDS['app_id'],
            api_key=KNACK_CREDS['api_key']
    )

    knack_data = kn.data
    
    if kits_data:
        new_data = datautil.merge_dicts(
            knack_data,
            kits_data,
            'KITS_ID',
            ['DMS_MESSAGE', 'MESSAGE_TIME']
        )

    for record in new_data:
        #  remove DMS formatting artifacts 
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[np]','\n')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[nl]',' ')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[pt40o0]','')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[pt30o0]','')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[fo13]','')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[fo2]','')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[jl3]','')
        record['DMS_MESSAGE'] = record['DMS_MESSAGE'].replace('[pt30]','')

        record[ CONFIG['modified_date_field'] ] = datautil.local_timestamp()

    new_data = datautil.reduce_to_keys(
        new_data,
        ['id','MESSAGE_TIME','DMS_MESSAGE', CONFIG['modified_date_field'] ])

    new_data = datautil.replace_keys(new_data,kn.field_map)

    count = 0

    for record in new_data:
        count += 1
        print( 'updating record {} of {}'.format( count, len(new_data) ) )

        res = knackpy.record(
            record,
            obj_key=CONFIG['ref_obj'][0],
            app_id=KNACK_CREDS['app_id'],
            api_key=KNACK_CREDS['api_key'],
            method='update',
        )

    return len(new_data)
           

if __name__=='__main__':
    try:

        script_name = os.path.basename(__file__).replace('.py', '')
        logfile = f'{LOG_DIRECTORY}/{script_name}.log'

        logger = logutil.timed_rotating_log(logfile)
        logger.info('START AT {}'.format( arrow.now() ))

        CONFIG = cfg['dms']
        KNACK_CREDS = KNACK_CREDENTIALS['data_tracker_prod']

        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='kits',
            destination='knack',
            auth=JOB_DB_API_TOKEN)

        job.start()

        results = main()

        job.result('success', records_processed=results)

        logger.info('END AT: {}'.format( arrow.now() ))
    

    except Exception as e:        
        error_text = traceback.format_exc()
        logger.error(error_text)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: DMS Message Update',
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )

        job.result('error', message=str(e) )

        raise e



