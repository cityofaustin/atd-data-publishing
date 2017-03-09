
#  sync master list of signal requests with open data portal

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


PRIMARY_KEY = 'ATD_EVAL_ID'


#  KNACK CONFIG
KNACK_PARAMS_TRAFFIC = {  
    'REFERENCE_OBJECTS' : ['object_13', 'object_27'],
    'SCENE' : '175',
    'VIEW' : '908',
    'FIELD_NAMES' : ['REQUEST_ID', 'EVAL_TYPE', 'REQUEST_STATUS', 'ATD_EVAL_ID', 'EVAL_STATUS', 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'LOCATION_NAME', 'LANDMARK', 'PRIMARY_ST_BLOCK', 'PRIMARY_ST_PREFIX_DIRECTION', 'PRIMARY_ST', 'CROSS_ST_BLOCK', 'CROSS_ST_PREFIX_DIRECTION', 'CROSS_ST', 'FUNDING_STATUS', 'EVAL_SCORE', 'EVAL_RANK', 'REQUEST_NOTE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

KNACK_PARAMS_PHB = {  
    'REFERENCE_OBJECTS' : ['object_13', 'object_26'],
    'SCENE' : '175',
    'VIEW' : '911',
    'FIELD_NAMES' : ['REQUEST_ID', 'EVAL_TYPE', 'REQUEST_STATUS', 'ATD_EVAL_ID', 'EVAL_STATUS', 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'LOCATION_NAME', 'LANDMARK', 'PRIMARY_ST_BLOCK', 'PRIMARY_ST_PREFIX_DIRECTION', 'PRIMARY_ST', 'CROSS_ST_BLOCK', 'CROSS_ST_PREFIX_DIRECTION', 'CROSS_ST', 'FUNDING_STATUS', 'EVAL_SCORE', 'EVAL_RANK', 'REQUEST_NOTE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

KNACK_PARAMS_REQ_LOCATIONS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_13'],
    'SCENE' : '175',
    'VIEW' : '923',
    'FIELD_NAMES' : ['REQUEST_ID', 'GEOCODE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = 'f6qu-b7zb'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'


now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       
        #  get and parse phb eval data
        field_list = knack_helpers.get_fields(KNACK_PARAMS_PHB)

        knack_data_phb = knack_helpers.get_data(KNACK_PARAMS_PHB)

        knack_data_phb = knack_helpers.parse_data(knack_data_phb, field_list, KNACK_PARAMS_PHB, convert_to_unix=True)

        knack_data_phb = data_helpers.stringify_key_values(knack_data_phb)
        
        knack_data_phb_mills = data_helpers.unix_to_mills(deepcopy(knack_data_phb))

        #  get and parse traffic eval data
        field_list = knack_helpers.get_fields(KNACK_PARAMS_TRAFFIC)

        knack_data_traffic = knack_helpers.get_data(KNACK_PARAMS_TRAFFIC)

        knack_data_traffic = knack_helpers.parse_data(knack_data_traffic, field_list, KNACK_PARAMS_TRAFFIC, convert_to_unix=True)

        knack_data_traffic = data_helpers.stringify_key_values(knack_data_traffic)
        
        knack_data_traffic_mills = data_helpers.unix_to_mills(deepcopy(knack_data_traffic))
        
        knack_data_master = knack_data_traffic_mills + knack_data_phb_mills
        
        #  get and parse location info
        field_list = knack_helpers.get_fields(KNACK_PARAMS_REQ_LOCATIONS)

        knack_data_req_loc = knack_helpers.get_data(KNACK_PARAMS_REQ_LOCATIONS)

        knack_data_req_loc = knack_helpers.parse_data(knack_data_req_loc, field_list, KNACK_PARAMS_REQ_LOCATIONS, convert_to_unix=True)

        knack_data_req_loc = data_helpers.stringify_key_values(knack_data_req_loc)

        #  append location info to eval data dicts
        knack_data_master = data_helpers.merge_dicts(knack_data_master, knack_data_req_loc, 'REQUEST_ID', ['LATITUDE', 'LONGITUDE'])

        #  get published request data from Socrata and compare to Knack database
        socrata_data = socrata_helpers.get_private_data(secrets.SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)

        socrata_data = data_helpers.upper_case_keys(socrata_data)
        
        socrata_data = data_helpers.stringify_key_values(socrata_data)
        
        socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)
        
        cd_results = data_helpers.detect_changes(socrata_data, knack_data_master, PRIMARY_KEY, keys=KNACK_PARAMS_TRAFFIC['FIELD_NAMES'] + ['LATITUDE', 'LONGITUDE'])

        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            socrata_payload = socrata_helpers.create_payload(cd_results, PRIMARY_KEY)

            socrata_payload = socrata_helpers.create_location_fields(socrata_payload)

        else:
            socrata_payload = []
        
        socrata_payload = data_helpers.lower_case_keys(socrata_payload)

        socrata_payload = data_helpers.unix_to_iso(socrata_payload)

        upsert_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_RESOURCE_ID)
                
        if 'error' in upsert_response:
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)
            
        elif upsert_response['Errors']:
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        log_payload = socrata_helpers.prep_pub_log(date_time, 'signal_request_master_list', upsert_response)

        pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e

results = main(now)

print(results)
