import logging
import pdb
import sys

import arrow
import knackpy
import datetime 

import _setpath
from config.secrets import *
from util import kitsutil
from util import datautil
from util import emailutil
from util import socratautil   

then = arrow.now()
now_s = then.format('YYYY_MM_DD')

logfile = '{}/dms_msg_pub_{}.log'.format(LOG_DIRECTORY, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(then)))

#  config
# primay_key = 'DMS_ID'
# knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
# ref_obj = ['object_109']
# scene = 'scene_569'
# view = 'view_1564'

    
def main(date_time):
    print('starting stuff now')

    try:      
        kits_query =  '''
            SELECT EVDATE as evdate
            ,NAME as name
            ,EVTYPE as type
            ,EVACTION as action
            ,DSC1 as bus
            FROM [KITS].[EVENTLOG]
            WHERE EVTYPE in ('EV Preempt','Vehicle Check In')
            '''

        kits_data = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            kits_query
        )
        pdb.set_trace()
        date_bins = {}
        for i in kits_data:
            curr_date = str(i['evdate'].year()) + ' ' + str(i['evdate'].month()) + ' ' + str(i['evdate'].day())
            if currdate not in list(date_bins.keys()):
                date_bins[curr_date] = 0
            date_bins[curr_date] += 1
        
        pdb.set_trace()
        # for record in kits_data:
        #     new_date = arrow.get(record['MESSAGE_TIME'])
        #     record['MESSAGE_TIME'] = new_date.timestamp * 1000


        # kn = knackpy.Knack(
        #         scene=scene,
        #         view=view,
        #         ref_obj=ref_obj,
        #         app_id=knack_creds['app_id'],
        #         api_key=knack_creds['api_key']
        # )


        
        
        # knack_data = kn.data
        # if kits_data:
        #     new_data = datautil.merge_dicts(
        #         knack_data,
        #         kits_data,
        #         'KITS_ID',
        #         ['DMS_MESSAGE', 'MESSAGE_TIME']
        #     )

            #new_data = datautil.stringify_key_values(new_data)


        

        # print('remove keys & re-name')
        # new_data = datautil.reduce_to_keys(new_data,['id','MESSAGE_TIME','DMS_MESSAGE'])
        # new_data = datautil.replace_keys(new_data,kn.field_map)


        


        # print('sending messages to Knack')
        # count = 0
        # for record in new_data:
        #     count += 1
        #     print( 'updating record {} of {}'.format( count, len(new_data) ) )

        #     response = knackpy.update_record(
        #         record,
        #         ref_obj[0],
        #         'id',
        #         knack_creds['app_id'],
        #         knack_creds['api_key']
        #     )
        #     print(response)
           

        

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        # emailutil.send_email(
        #     ALERTS_DISTRIBUTION,
        #     'DATA PROCESSING ALERT: DMS Message Update',
        #     str(e),
        #     EMAIL['user'],
        #     EMAIL['password']
        # )

        raise e
 
results = main(then)

print(results['res'])
logging.info('Elapsed time: {}'.format(str(arrow.now() - then)))
