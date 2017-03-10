
#  publish sync signal corridor data to Socrata

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
from copy import deepcopy
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
import secrets
import pdb

PRIMARY_KEY = 'ATD_RETIMING_ID'


#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_42', 'object_45'],
    'SCENE' : '375',
    'VIEW' : '1063',
    'FIELD_NAMES' : ['ATD_RETIMING_ID', 'SYSTEM_ID', 'SYSTEM_NAME', 'SCHEDULED_FY', 'RETIME_STATUS', 'STATUS_DATE', 'TOTAL_VOL', 'VOL_WAVG_TT_PCT_CHANGE', 'VOL_WAVG_TT_SECONDS', 'SIGNAL_COUNT', 'ENGINEER_NOTE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}


#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = 'g8w2-8uap'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'


now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       
        # get and parse knack data
        field_list = knack_helpers.get_fields(KNACK_PARAMS)

        knack_data = knack_helpers.get_data(KNACK_PARAMS)

        knack_data = knack_helpers.parse_data(knack_data, field_list, KNACK_PARAMS, convert_to_unix=True)

        knack_data = data_helpers.stringify_key_values(knack_data)
        
        knack_data_mills = data_helpers.unix_to_mills(deepcopy(knack_data))

        # get published request data from Socrata and compare to Knack database
        # socrata_data = socrata_helpers.get_private_data(secrets.SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)

        # socrata_data = data_helpers.upper_case_keys(socrata_data)
        
        # socrata_data = data_helpers.stringify_key_values(socrata_data)
        
        # socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)
        
        socrata_payload = knack_data_mills
        
        # cd_results = data_helpers.detect_changes(socrata_data, knack_data_mills, PRIMARY_KEY, keys=KNACK_PARAMS['FIELD_NAMES'] + ['LATITUDE', 'LONGITUDE'])

        # if cd_results['new'] or cd_results['change'] or cd_results['delete']:
        #     socrata_payload = socrata_helpers.create_payload(cd_results, PRIMARY_KEY)
        
        # else:
        #     socrata_payload = []

        socrata_payload = data_helpers.lower_case_keys(socrata_payload)

        socrata_payload = data_helpers.unix_to_iso(socrata_payload)

        upsert_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_RESOURCE_ID)
                
        if 'error' in upsert_response:
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)
            
        elif upsert_response['Errors']:
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        log_payload = socrata_helpers.prep_pub_log(date_time, 'signal_retiming', upsert_response)

        pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        return upsert_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e

results = main(now)

print(results)
