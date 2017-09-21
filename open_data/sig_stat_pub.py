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

socrata_signals = 'xwqn-2f78'
socrata_signal_status = '5zpr-dehc'
socrata_signal_status_historical = 'x62n-vjpq'
socrata_pub_log_id = 'n5kp-f8k4'
flash_statuses = ['1', '2', '3']
socrata_historical_fields = ['signal_id', 'operation_state_duration', 'operation_state', 'record_retired_datetime', 'record_id', 'location_name', 'processed_datetime', 'operation_state_datetime']

then = arrow.now()
now_s = then.format('YYYY_MM_DD')

logfile = '{}/sig_stat_pub_{}.log'.format(LOG_DIRECTORY, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(then)))
    
def main(date_time):
    print('starting stuff now')

    try:      
        socr = socratautil.Soda(
            socrata_signals,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password']
        )
        
        socr.get_metadata()
        signal_data = datautil.upper_case_keys(socr.data)
        
        kits_query = kitsutil.generate_status_id_query(
            signal_data,
            'SIGNAL_ID'
        )

        kits_data = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            kits_query
        )
        
        kits_data = datautil.replaceTimezone(kits_data)
        
        kits_data = datautil.stringify_key_values(kits_data)
        
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
                socrata_pub_log_id
            )

            sys.exit()

        kits_data = datautil.filter_by_val(
            kits_data,
            'OPERATION_STATE',
            flash_statuses
        )
        
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
        
        sig_status = socratautil.Soda(
            socrata_signal_status,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password']
        )
        
        sig_status.get_metadata()
        fieldnames = sig_status.fieldnames
        fieldnames.append(':deleted')  #  add socrata deleted field for record deletes
        sig_status_data = datautil.reduce_to_keys(sig_status.data, fieldnames)
        date_fields = sig_status.date_fields
        sig_status_data = socratautil.strip_geocoding(sig_status_data)
        sig_status_data = datautil.upper_case_keys(sig_status_data)
        sig_status_data = datautil.stringify_key_values(sig_status_data)

        cd_results = datautil.detect_changes(
            sig_status_data,
            new_data,
            'SIGNAL_ID',
            #  only a change in operation state
            #  triggers an update to socrata dataset
            keys=['OPERATION_STATE']  
        )

        for change_type in cd_results.keys():
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
                socrata_payload
            )

            socrata_payload = datautil.lower_case_keys(
                socrata_payload
            )

            socrata_payload = datautil.reduce_to_keys(socrata_payload, fieldnames)
            
            status_upsert_response = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                socrata_payload,
                socrata_signal_status
            )
        
        else:
            status_upsert_response = {
                'Errors' : 0,
                'message' : 'No signal status change detected',
                'Rows Updated' : 0,
                'Rows Created' : 0,
                'Rows Deleted' : 0
            }

        log_payload = socratautil.prep_pub_log(
            date_time,
            'signal_status_update',
            status_upsert_response
        )


        pub_log_response = socratautil.upsert_data(
            SOCRATA_CREDENTIALS,
            log_payload,
            socrata_pub_log_id
        )

        
        if 'error' in status_upsert_response:
            logging.info('socrata error')
            logging.info(socrata_payload)
            emailutil.send_socrata_alert(
                ALERTS_DISTRIBUTION,
                socrata_signal_status,
                status_upsert_response,
                EMAIL['user'],
                EMAIL['password']
            )
            
        elif status_upsert_response['Errors']:
            logging.info('socrata Errors')
            logging.info(socrata_payload)
            emailutil.send_socrata_alert(
                ALERTS_DISTRIBUTION,
                socrata_signal_status,
                status_upsert_response,
                EMAIL['user'],
                EMAIL['password']
            )

        if cd_results['delete']:
            historical_payload = datautil.lower_case_keys(
                cd_results['delete']
            )

            historical_payload = socratautil.add_hist_fields(historical_payload)

            historical_payload = datautil.reduce_to_keys(historical_payload, socrata_historical_fields)

            status_upsert_historical_response = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                historical_payload,
                socrata_signal_status_historical
            )

            historical_log_payload = socratautil.prep_pub_log(
                date_time,
                'signal_status_historical_update',
                status_upsert_historical_response
            )

            pub_log_historical_response = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                historical_log_payload,
                socrata_pub_log_id
            )

            if 'error' in status_upsert_historical_response:
                loggig.info('socrata error historical dataset')
                logging.info(socrata_payload)
                emailutil.send_socrata_alert(
                    ALERTS_DISTRIBUTION,
                    socrata_signal_status_historical,
                    status_upsert_historical_response,
                    EMAIL['user'],
                    EMAIL['password']
                )
                
            elif status_upsert_historical_response['Errors']:
                logging.info('socrata error historical dataset')
                logging.info(socrata_payload)
                emailutil.send_socrata_alert(
                    ALERTS_DISTRIBUTION,
                    socrata_signal_status_historical,
                    status_upsert_historical_response,
                    EMAIL['user'],
                    EMAIL['password']
                )

        else:
            print('no new historical status data to upload')
            status_upsert_historical_response = None

        return {
            'res': status_upsert_response,
            'res_historical': status_upsert_historical_response,
        }
    
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

print(results['res'])
logging.info('Elapsed time: {}'.format(str(arrow.now() - then)))
