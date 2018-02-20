import logging
import os
import pdb
import sys

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import kitsutil
from util import datautil
from util import emailutil
from util import socratautil   

then = arrow.now()

script = os.path.basename(__file__).replace('.py', '.log')
logfile = f'{LOG_DIRECTORY}/{script}'
logging.basicConfig(filename=logfile, level=logging.INFO)

#  config
primay_key = 'DMS_ID'
knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
ref_obj = ['object_109']
scene = 'scene_569'
view = 'view_1564'

    
def main(date_time):
    print('starting stuff now')

    try:      
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
                scene=scene,
                view=view,
                ref_obj=ref_obj,
                app_id=knack_creds['app_id'],
                api_key=knack_creds['api_key']
        )

        knack_data = kn.data
        if kits_data:
            new_data = datautil.merge_dicts(
                knack_data,
                kits_data,
                'KITS_ID',
                ['DMS_MESSAGE', 'MESSAGE_TIME']
            )

            #new_data = datautil.stringify_key_values(new_data)
        print('chop and replace')
        for msg in new_data:
            #chop off first formatting
            #msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'][19:]
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[np]','\n')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[nl]',' ')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[pt40o0]','')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[pt30o0]','')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[fo13]','')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[fo2]','')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[jl3]','')
            msg['DMS_MESSAGE'] = msg['DMS_MESSAGE'].replace('[pt30]','')

        print('remove keys & re-name')
        new_data = datautil.reduce_to_keys(new_data,['id','MESSAGE_TIME','DMS_MESSAGE'])
        new_data = datautil.replace_keys(new_data,kn.field_map)

        print('sending messages to Knack')
        count = 0
        for record in new_data:
            count += 1
            print( 'updating record {} of {}'.format( count, len(new_data) ) )

            res = knackpy.record(
                record,
                obj_key=ref_obj[0],
                app_id= knack_creds['app_id'],
                api_key=knack_creds['api_key'],
                method='update',
            )
           
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: DMS Message Update',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e
 
results = main(then)

logging.info('Elapsed time: {}'.format(str(arrow.now() - then)))
