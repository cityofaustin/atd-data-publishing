import logging
import os
import pdb
import sys

import arrow

import _setpath
from config.knack.config import cfg
from config.secrets import *
from util import kitsutil
from util import datautil
from util import emailutil
from util import socratautil

SOCR_SIG_RES_ID = 'xwqn-2f78'
SOCR_SIG_STAT_RES_ID = '5zpr-dehc'
PUB_LOG_ID = cfg['publication_log']['socrata_resource_id']
DATASET = 'Traffic Signal Status'
FLASH_STATUSES = ['1', '2', '3']

script_name = __file__.split('.')[0]
start_time = arrow.now()
script = os.path.basename(__file__).replace('.py', '.log')
logfile = f'{LOG_DIRECTORY}/{script}'
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(start_time)))
    

def update_pub_log(upsert_response):
    '''
    Update data publication log with script outcome
    '''
    log_payload = socratautil.pub_log_payload(
        script_name,  #  id
        start_time.timestamp,  #  start
        arrow.now().timestamp,  #  end
        resource=SOCR_SIG_STAT_RES_ID,
        dataset=DATASET
    )

    #  update pub log payload with data from upsert response
    log_payload = socratautil.handle_response(upsert_response, log_payload)

    #  upsert pub log payload
    pub_log_response = socratautil.upsert_data(
        SOCRATA_CREDENTIALS,
        log_payload,
        PUB_LOG_ID
    )


def main(date_time):
    print('starting stuff now')

    try:
        # get current traffic signal data from Socrata      
        socr = socratautil.Soda(
            SOCR_SIG_RES_ID,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password']
        )
        
        socr.get_data()
        socr.get_metadata()
        
        signal_data = datautil.upper_case_keys(socr.data)

        kits_query = kitsutil.status_query()

        kits_data = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            kits_query
        )
        
        kits_data = datautil.replaceTimezone(kits_data, ['OPERATION_STATE_DATETIME'])

        kits_data = datautil.stringify_key_values(kits_data)
        
        #  verify the KITS data is current
        #  sometimes the signal status service goes down
        #  in which case contact ATMS support
        stale = kitsutil.check_for_stale(
            kits_data,
            'OPERATION_STATE_DATETIME',15
        )

        if stale['stale']:
            emailutil.send_stale_email(
                stale['delta_minutes'],
                ALERTS_DISTRIBUTION,
                EMAIL['user'],
                EMAIL['password']
            )

            response_obj = {
                'Errors' : 1,
                'message' : 'WARNING: stale data detected',
                'Rows Updated' : 0,
                'Rows Created' : 0,
                'Rows Deleted' : 0
            }

            update_pub_log(response_obj)
            sys.exit()

        #  filter KITS data for statuses of concern
        kits_data = datautil.filter_by_val(
            kits_data,
            'OPERATION_STATE',
            FLASH_STATUSES
        )

        #  append kits data to signal data
        if kits_data:
            new_data = datautil.merge_dicts(
                signal_data,
                kits_data,
                'SIGNAL_ID',
                ['OPERATION_STATE_DATETIME', 'OPERATION_STATE', 'PLAN_ID']
            )

            new_data = datautil.stringify_key_values(new_data)

        else:
            new_data = []
        
        #  get current signal status DATASET and metadata from socrata
        sig_status = socratautil.Soda(
            SOCR_SIG_STAT_RES_ID
        )

        sig_status.get_data()
        sig_status.get_metadata()
        
        #  add special socrata deleted field
        #  required for sending delete requests to socrata
        fieldnames = sig_status.fieldnames + [':deleted']

        #  transform signal status socrata data for comparison 
        #  with "new" data from kits
        sig_status_data = datautil.reduce_to_keys(sig_status.data, fieldnames)
        date_fields = sig_status.date_fields
        sig_status_data = socratautil.strip_geocoding(sig_status_data)
        sig_status_data = datautil.upper_case_keys(sig_status_data)
        sig_status_data = datautil.stringify_key_values(sig_status_data)

        #  identify signals whose status (OPERATION_STATE) has changed
        cd_results = datautil.detect_changes(
            sig_status_data,
            new_data,
            'SIGNAL_ID',
            #  only a change in operation state
            #  triggers an update to socrata DATASET
            keys=['OPERATION_STATE']  
        )

        for change_type in cd_results.keys():
            #  log signals whose status has changed
            if len(cd_results[change_type]) > 0:
                logging.info(
                    '{}: {}'.format(change_type, len(cd_results[change_type]))
                )
    
            
        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            
            socrata_payload = socratautil.create_payload(
                cd_results,
                'SIGNAL_ID'
            )

            socrata_payload = datautil.lower_case_keys(
                socrata_payload
            )

            socrata_payload = socratautil.create_location_fields(
                socrata_payload,
                lat_field='location_latitude',
                lon_field='location_longitude',
                location_field='location'
            )

            socrata_payload = datautil.reduce_to_keys(
                socrata_payload,
                fieldnames
            )

            upsert_res = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                socrata_payload,
                SOCR_SIG_STAT_RES_ID
            )
            
        else:
            upsert_res = {
                'Errors' : 0,
                'message' : 'No signal status change detected',
                'Rows Updated' : 0,
                'Rows Created' : 0,
                'Rows Deleted' : 0
            }
    
        update_pub_log(upsert_res)

        if upsert_res.get('error') or upsert_res.get('Errors'):
            raise Exception(str(upsert_res))

        return upsert_res
    
    except Exception as e:
        logging.info(e)
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: Signal Status Update Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e
 
results = main(start_time)

print(results)
logging.info('Elapsed time: {}'.format(str(arrow.now() - start_time)))
