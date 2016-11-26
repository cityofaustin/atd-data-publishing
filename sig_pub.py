#  sync signal data in asset database with Socrata, ArcGIS Online

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
from secrets import KNACK_CREDENTIALS
from secrets import AGOL_CREDENTIALS
from secrets import SOCRATA_CREDENTIALS
from secrets import SOCRATA_CREDENTIALS
from secrets import ALERTS_DISTRIBUTION
import pdb

PRIMARY_KEY = 'ATD_SIGNAL_ID'

#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_12'],
    'SCENE' : '73',
    'VIEW' : '197',
    'FIELD_NAMES' : ['ATD_LOCATION_ID','ATD_SIGNAL_ID','COA_INTERSECTION_ID','CONTROL','COUNCIL_DISTRICT', 'CROSS_ST','CROSS_ST_AKA','CROSS_ST_SEGMENT_ID','JURISDICTION','LANDMARK','LOCATION_NAME','PRIMARY_ST', 'PRIMARY_ST_AKA','PRIMARY_ST_SEGMENT_ID','SIGNAL_ENG_AREA','SIGNAL_STATUS','SIGNAL_TYPE','TRAFFIC_ENG_AREA','MASTER_SIGNAL_ID', 'GEOCODE', 'IP_SWITCH', 'IP_CONTROL', 'SWITCH_COMM', 'COMM_PLAN', 'TURN_ON_DATE', 'MODIFIED_DATE'],
    'OUT_FIELDS' : ['ATD_LOCATION_ID','ATD_SIGNAL_ID','COA_INTERSECTION_ID','CONTROL','COUNCIL_DISTRICT', 'CROSS_ST','CROSS_ST_AKA','CROSS_ST_SEGMENT_ID','JURISDICTION','LANDMARK','LOCATION_NAME','PRIMARY_ST', 'PRIMARY_ST_AKA','PRIMARY_ST_SEGMENT_ID','SIGNAL_ENG_AREA','SIGNAL_STATUS','SIGNAL_TYPE','TRAFFIC_ENG_AREA','MASTER_SIGNAL_ID', 'LATITUDE', 'LONGITUDE', 'IP_SWITCH', 'IP_CONTROL', 'SWITCH_COMM', 'COMM_PLAN', 'TURN_ON_DATE', 'MODIFIED_DATE'],
    'APPLICATION_ID' : KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : KNACK_CREDENTIALS['API_KEY']
}

#  AGOL CONFIG
SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_signals2/FeatureServer/0/'

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = 'p53x-x73x'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'

now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       

        field_list = knack_helpers.GetFields(KNACK_PARAMS)

        knack_data = knack_helpers.GetData(KNACK_PARAMS)

        knack_data_parsed = knack_helpers.ParseData(knack_data, field_list, KNACK_PARAMS)

        knack_data_parsed = data_helpers.StringifyKeyValues(knack_data_parsed)
        
        agol_payload = agol_helpers.BuildPayload(knack_data_parsed)

        token = agol_helpers.GetToken(AGOL_CREDENTIALS)

        del_response = agol_helpers.DeleteFeatures(SERVICE_URL, token)

        add_response = agol_helpers.AddFeatures(SERVICE_URL, token, agol_payload)

        socrata_data = socrata_helpers.FetchPrivateData(SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)

        socrata_data = data_helpers.UpperCaseKeys(socrata_data)

        socrata_data = data_helpers.StringifyKeyValues(socrata_data)

        cd_results = data_helpers.DetectChanges(socrata_data, knack_data_parsed, PRIMARY_KEY)

        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            socrata_payload = socrata_helpers.CreatePayload(cd_results, PRIMARY_KEY)

            socrata_payload = socrata_helpers.CreateLocationFields(socrata_payload)

        else:
            socrata_payload = []

        upsert_response = socrata_helpers.UpsertData(SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_RESOURCE_ID)

        if 'error' in upsert_response:
            email_helpers.SendSocrataAlert(ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)
            
        elif upsert_response['Errors']:
            email_helpers.SendSocrataAlert(ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        log_payload = socrata_helpers.PrepPubLog(date_time, 'signals_update', upsert_response)

        pub_log_response = socrata_helpers.UpsertData(SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)
