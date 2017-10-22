import logging
import pdb
import sys

import arrow

import _setpath
from config.secrets import *
from util import kitsutil
from util import datautil
from util import emailutil
from util import socratautil

socr_sig_res_id = 'xwqn-2f78'
socr_sig_stat_res_id = '5zpr-dehc'
socr_pub_log_res_id = 'n5kp-f8k4'
flash_statuses = ['1', '2', '3']

then = arrow.now()
now_s = then.format('YYYY_MM_DD')

logfile = '{}/sig_stat_pub_{}.log'.format(LOG_DIRECTORY, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(then)))
    

def main(date_time):
    print('starting stuff now')

    try:
        # get current traffic signal data from Socrata      
        socr = socratautil.Soda(
            socr_sig_res_id,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password']
        )
        
        #  signal metadata and transform for KITS query
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

            stale_data_log = socratautil.prep_pub_log(
                date_time,
                'signal_status_update',
                response_obj
            )
            
            socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                stale_data_log,
                socr_pub_log_res_id
            )

            sys.exit()

        #  filter KITS data for statuses of concern
        kits_data = datautil.filter_by_val(
            kits_data,
            'OPERATION_STATE',
            flash_statuses
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
        

        #  get current signal status dataset and metadata from socrata
        sig_status = socratautil.Soda(
            socr_sig_stat_res_id,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password']
        )
        
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
            #  triggers an update to socrata dataset
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

            socrata_payload = socratautil.create_location_fields(
                socrata_payload,
                lat_field='LOCATION_LATITUDE',
                lon_field='LOCATION_LONGITUDE'
            )

            socrata_payload = datautil.lower_case_keys(
                socrata_payload
            )

            socrata_payload = datautil.reduce_to_keys(
                socrata_payload,
                fieldnames
            )

            upsert_res = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                socrata_payload,
                socr_sig_stat_res_id
            )

        else:
            upsert_res = {
                'Errors' : 0,
                'message' : 'No signal status change detected',
                'Rows Updated' : 0,
                'Rows Created' : 0,
                'Rows Deleted' : 0
            }

        log_payload = socratautil.prep_pub_log(
            date_time,
            'signal_status_update',
            upsert_res
        )

        pub_log_res = socratautil.upsert_data(
            SOCRATA_CREDENTIALS,
            log_payload,
            socr_pub_log_res_id
        )

        if 'error' in upsert_res:
            logging.info('socrata error')
            logging.info(socrata_payload)
            emailutil.send_socrata_alert(
                ALERTS_DISTRIBUTION,
                socr_sig_stat_res_id,
                upsert_res,
                EMAIL['user'],
                EMAIL['password']
            )
            
        elif upsert_res['Errors']:
            ('socrata Errors')
            logging.info(socrata_payload)
            emailutil.send_socrata_alert(
                ALERTS_DISTRIBUTION,
                socr_sig_stat_res_id,
                upsert_res,
                EMAIL['user'],
                EMAIL['password']
            )

        return upsert_res
    
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: Signal Status Update Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e
 

results = main(then)

print(results)
logging.info('Elapsed time: {}'.format(str(arrow.now() - then)))
