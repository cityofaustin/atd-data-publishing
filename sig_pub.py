
#  sync signal data in asset database with Socrata, ArcGIS Online

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

PRIMARY_KEY = 'ATD_SIGNAL_ID'

#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_12'],
    'SCENE' : '73',
    'VIEW' : '197',
    'FIELD_NAMES' : ['ATD_LOCATION_ID','ATD_SIGNAL_ID','COA_INTERSECTION_ID','CONTROL','COUNCIL_DISTRICT', 'CROSS_ST','CROSS_ST_AKA','CROSS_ST_SEGMENT_ID','JURISDICTION','LANDMARK','LOCATION_NAME','PRIMARY_ST', 'PRIMARY_ST_AKA','PRIMARY_ST_SEGMENT_ID','SIGNAL_ENG_AREA','SIGNAL_STATUS','SIGNAL_TYPE','TRAFFIC_ENG_AREA','MASTER_SIGNAL_ID', 'GEOCODE', 'IP_SWITCH', 'IP_CONTROL', 'SWITCH_COMM', 'COMM_PLAN', 'TURN_ON_DATE', 'MODIFIED_DATE', 'CROSS_ST_BLOCK', 'PRIMARY_ST_BLOCK', 'COUNTY'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

#  AGOL CONFIG
SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_signals2/FeatureServer/0/'

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = 'p53x-x73x'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'

#  CSV OUTPUT
CSV_DESTINATION = secrets.FME_DIRECTORY
DATASET_NAME = 'atd_signals'

now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       

        field_list = knack_helpers.get_fields(KNACK_PARAMS)

        knack_data = knack_helpers.get_data(KNACK_PARAMS)

        knack_data = knack_helpers.parse_data(knack_data, field_list, KNACK_PARAMS, require_locations=True, convert_to_unix=True)

        knack_data = data_helpers.stringify_key_values(knack_data)

        knack_data = data_helpers.remove_linebreaks(knack_data, ['LOCATION_NAME']) 

        knack_data_mills = data_helpers.unix_to_mills(deepcopy(knack_data))
        
        token = agol_helpers.get_token(secrets.AGOL_CREDENTIALS)

        agol_payload = agol_helpers.build_payload(knack_data_mills)

        del_response = agol_helpers.delete_features(SERVICE_URL, token)

        add_response = agol_helpers.add_features(SERVICE_URL, token, agol_payload)

        socrata_data = socrata_helpers.get_private_data(secrets.SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)

        socrata_data = data_helpers.upper_case_keys(socrata_data)

        socrata_data = data_helpers.stringify_key_values(socrata_data)

        socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)

        cd_results = data_helpers.detect_changes(socrata_data, knack_data, PRIMARY_KEY, keys=KNACK_PARAMS['FIELD_NAMES']  + ['LATITUDE', 'LONGITUDE'])

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

        log_payload = socrata_helpers.prep_pub_log(date_time, 'signals_update', upsert_response)

        pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        #  write to csv
        knack_data = data_helpers.unix_to_iso(knack_data)
        file_name = '{}/{}.csv'.format(CSV_DESTINATION, DATASET_NAME)
        data_helpers.write_csv(knack_data, file_name=file_name)
        
        return log_payload
        # return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)







