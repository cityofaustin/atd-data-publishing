
#  publish sync signal corridor data to Socrata

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from copy import deepcopy
import logging
import pdb
import arrow
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
import secrets

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

#  init logging with one logfile per dataset per day
logfile = './log/sync_corridors_{}.log'.format(now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

#  KNACK CONFIG
primary_key = 'ATD_SYNC_SIGNAL_ID'

knack_creds = secrets.KNACK_CREDENTIALS

params_sync_signals = {  
    'objects' : ['object_12', 'object_42', 'object_43'],
    'scene' : '277',
    'view' : '765',
    'field_names' : ['ATD_SYNC_SIGNAL_ID', 'SIGNAL_ID', 'LOCATION_NAME', 'SYSTEM_ID', 'SYSTEM_NAME', 'LIMIT', 'ISOLATED']
}

params_locations = {  
    'objects' : ['object_11', 'object_12'],
    'scene' : '73',
    'view' : '197',
    'field_names' : ['SIGNAL_ID', 'GEOCODE']
}

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = 'efct-8fs9'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'

def main(date_time):
    print('starting stuff now')

    try:       
        #  get and corridor data
        field_dict = knack_helpers.get_fields(params_sync_signals['objects'], knack_creds)
        knack_data_sync_signals = knack_helpers.get_data(params_sync_signals['scene'], params_sync_signals['view'], knack_creds)
        knack_data_sync_signals = knack_helpers.parse_data(knack_data_sync_signals, field_dict, convert_to_unix=True)
        knack_data_sync_signals = data_helpers.stringify_key_values(knack_data_sync_signals)
        knack_data_sync_signals_mills = data_helpers.unix_to_mills(deepcopy(knack_data_sync_signals))
        
        #  get and parse location info
        field_dict = knack_helpers.get_fields(params_locations['objects'], knack_creds)
        knack_data_loc = knack_helpers.get_data(params_locations['scene'], params_locations['view'], knack_creds)
        knack_data_loc = knack_helpers.parse_data(knack_data_loc, field_dict, convert_to_unix=True)
        knack_data_loc = data_helpers.stringify_key_values(knack_data_loc)

        #  append location info to corridor data
        knack_data_master = data_helpers.merge_dicts(knack_data_sync_signals_mills, knack_data_loc, 'SIGNAL_ID', ['LATITUDE', 'LONGITUDE'])

        print(knack_data_master[0:3])

        #  get published data from Socrata and compare to Knack database
        socrata_data = socrata_helpers.get_private_data(secrets.SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)
        socrata_data = data_helpers.upper_case_keys(socrata_data)
        socrata_data = data_helpers.stringify_key_values(socrata_data)
        socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)
        
        cd_results = data_helpers.detect_changes(socrata_data, knack_data_master, primary_key, keys=params_sync_signals['field_names'] + ['LATITUDE', 'LONGITUDE'])
        
        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            logging.info( 'socrata change detection results: {}'.format(cd_results) )
            socrata_payload = socrata_helpers.create_payload(cd_results, primary_key)

        else:
            socrata_payload = []
            logging.info( 'no socrata payload')
        
        socrata_payload = data_helpers.lower_case_keys(socrata_payload)
        socrata_payload = data_helpers.unix_to_iso(socrata_payload)
        upsert_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_RESOURCE_ID)
                
        if 'error' in upsert_response:
            logging.error( 'socrata upsert error: {}'.format(upsert_response) )
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)
            
        elif upsert_response['Errors']:
            logging.error( 'socrata upsert error: {}'.format(upsert_response) )
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        log_payload = socrata_helpers.prep_pub_log(date_time, 'sync_signal_corridors', upsert_response)
        pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        return upsert_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        logging.error(str(e))
        raise e

results = main(now)

logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)
