#  sync sensor data in asset database with Socrata, ArcGIS Online

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

PRIMARY_KEY = 'ATD_SENSOR_ID'

#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_56'],
    'SCENE' : '188',
    'VIEW' : '540',
    'FIELD_NAMES' : ['ATD_SENSOR_ID', 'SENSOR_MFG', 'TURN_ON_DATE', 'ATD_LOCATION_ID', 'PRIMARY_ST_SEGMENT_ID', 'CROSS_ST_SEGMENT_ID', 'LANDMARK', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'JURISDICTION', 'LOCATION_TYPE', 'LOCATION_NAME', 'PRIMARY_ST', 'CROSS_ST', 'MODIFIED_DATE', 'PRIMARY_ST_BLOCK', 'COA_INTERSECTION_ID', 'SENSOR_STATUS', 'COUNTY', 'CROSS_ST_AKA', 'CROSS_ST_BLOCK', 'PRIMARY_ST_AKA', 'GEOCODE', 'SENSOR_TYPE', 'READER_ID', 'LATITUDE', 'LONGTITUDE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}



#  AGOL CONFIG
SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_signals2/FeatureServer/0/'

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = '6yd9-yz29'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'


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

        # token = agol_helpers.get_token(secrets.AGOL_CREDENTIALS)

        # agol_payload = agol_helpers.build_payload(knack_data_mills)

        # del_response = agol_helpers.delete_features(SERVICE_URL, token)

        # add_response = agol_helpers.add_features(SERVICE_URL, token, agol_payload)

        socrata_data = socrata_helpers.get_private_data(secrets.SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)

        socrata_data = data_helpers.upper_case_keys(socrata_data)

        socrata_data = data_helpers.stringify_key_values(socrata_data)

        socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)

        cd_results = data_helpers.detect_changes(socrata_data, knack_data, PRIMARY_KEY, keys=KNACK_PARAMS['FIELD_NAMES'] + ['LATITUDE', 'LONGITUDE'])

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

        log_payload = socrata_helpers.prep_pub_log(date_time, 'sensors_update', upsert_response)

        pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)

