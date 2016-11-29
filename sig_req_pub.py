#  sync signal request data in asset database with Socrata, ArcGIS Online
#  skip socrata publish until new dataset is defined

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
import secrets
import pdb

PRIMARY_KEY = 'REQUEST_ID'

#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_13'],
    'SCENE' : '75',
    'VIEW' : '200',
    'FIELD_NAMES' : ['REQUEST_ID', 'ATD_LOCATION_ID', 'REQUEST_TYPE', 'FUNDING_STATUS', 'REQUEST_STATUS', 'PRIMARY_ST_SEGMENT_ID', 'CROSS_ST_SEGMENT_ID', 'GEOCODE', 'LANDMARK', 'PRIMARY_ST_AKA', 'CROSS_ST_AKA', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'JURISDICTION', 'LOCATION_NAME', 'PRIMARY_ST', 'CROSS_ST', 'COA_INTERSECTION_ID', 'REQUEST_DATE'],
    'OUT_FIELDS' : ['REQUEST_ID', 'ATD_LOCATION_ID', 'REQUEST_TYPE', 'FUNDING_STATUS', 'REQUEST_STATUS', 'PRIMARY_ST_SEGMENT_ID', 'CROSS_ST_SEGMENT_ID', 'LATITUDE', 'LONGITUDE', 'LANDMARK', 'PRIMARY_ST_AKA', 'CROSS_ST_AKA', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'JURISDICTION', 'LOCATION_NAME', 'PRIMARY_ST', 'CROSS_ST', 'COA_INTERSECTION_ID', 'REQUEST_DATE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

#  AGOL CONFIG
SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_signal_requests/FeatureServer/0/'

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = ''
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'

now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       

        field_list = knack_helpers.GetFields(KNACK_PARAMS)

        knack_data = knack_helpers.GetData(KNACK_PARAMS)

        knack_data = knack_helpers.ParseData(knack_data, field_list, KNACK_PARAMS)

        knack_data = data_helpers.StringifyKeyValues(knack_data)
        
        token = agol_helpers.GetToken(secrets.AGOL_CREDENTIALS)

        agol_payload = agol_helpers.BuildPayload(knack_data, convertUnixDates=True )

        del_response = agol_helpers.DeleteFeatures(SERVICE_URL, token)

        add_response = agol_helpers.AddFeatures(SERVICE_URL, token, agol_payload)

        # socrata_data = socrata_helpers.FetchPrivateData(secrets.SOCRATA_CREDENTIALS, SOCRATA_RESOURCE_ID)

        # socrata_data = data_helpers.UpperCaseKeys(socrata_data)

        # socrata_data = data_helpers.StringifyKeyValues(socrata_data)

        # socrata_data = socrata_helpers.ConvertToUnix(socrata_data)

        # cd_results = data_helpers.DetectChanges(socrata_data, knack_data, PRIMARY_KEY)

        # if cd_results['new'] or cd_results['change'] or cd_results['delete']:
        #     socrata_payload = socrata_helpers.CreatePayload(cd_results, PRIMARY_KEY)

        #     socrata_payload = socrata_helpers.CreateLocationFields(socrata_payload)

        # else:
        #     socrata_payload = []

        # upsert_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_RESOURCE_ID)

        # if 'error' in upsert_response:
        #     email_helpers.SendSocrataAlert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)
            
        # elif upsert_response['Errors']:
        #     email_helpers.SendSocrataAlert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        # log_payload = socrata_helpers.PrepPubLog(date_time, 'signals_update', upsert_response)

        # pub_log_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        # return log_payload

        return add_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)
