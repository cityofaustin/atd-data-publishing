#  holding off on socrata pub until new dataset is defined

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
from secrets import KNACK_CREDENTIALS
from secrets import AGOL_CREDENTIALS
from secrets import SOCRATA_CREDENTIALS
from secrets import SOCRATA_CREDENTIALS
from secrets import ALERTS_DISTRIBUTION
import pdb


#  KNACK CONFIG
SCENE = '75'
VIEW = '200'

REFERENCE_OBJECTS = ['object_11', 'object_13']

KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : REFERENCE_OBJECTS,
    'FIELD_NAMES' : ['REQUEST_ID', 'ATD_LOCATION_ID', 'REQUEST_TYPE', 'FUNDING_STATUS', 'REQUEST_STATUS', 'PRIMARY_ST_SEGMENT_ID', 'CROSS_ST_SEGMENT_ID', 'GEOCODE', 'LANDMARK', 'PRIMARY_ST_AKA', 'CROSS_ST_AKA', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'JURISDICTION', 'LOCATION_NAME', 'PRIMARY_ST', 'CROSS_ST', 'COA_INTERSECTION_ID', 'REQUEST_DATE'],
    'OUT_FIELDS' : ['REQUEST_ID', 'ATD_LOCATION_ID', 'REQUEST_TYPE', 'FUNDING_STATUS', 'REQUEST_STATUS', 'PRIMARY_ST_SEGMENT_ID', 'CROSS_ST_SEGMENT_ID', 'LATITUDE', 'LONGITUDE', 'LANDMARK', 'PRIMARY_ST_AKA', 'CROSS_ST_AKA', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'JURISDICTION', 'LOCATION_NAME', 'PRIMARY_ST', 'CROSS_ST', 'COA_INTERSECTION_ID', 'REQUEST_DATE'],
    'OBJECTS_URL' : 'https://api.knack.com/v1/objects/',
    'TABLE_URL' : 'https://api.knack.com/v1/pages/scene_{}/views/view_{}/records?rows_per_page=1000'.format( SCENE, VIEW ),
    'APPLICATION_ID' : KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : KNACK_CREDENTIALS['API_KEY']
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

        knack_data_parsed = knack_helpers.ParseData(knack_data, field_list, KNACK_PARAMS)

        agol_payload = agol_helpers.BuildPayload(knack_data_parsed)

        token = agol_helpers.GetToken(AGOL_CREDENTIALS)

        del_response = agol_helpers.DeleteFeatures(SERVICE_URL, token)

        add_response = agol_helpers.AddFeatures(SERVICE_URL, token, agol_payload)

        #  upsert_response = socrata_helpers.UpsertData(SOCRATA_CREDENTIALS, knack_data_parsed, SOCRATA_RESOURCE_ID)

        #  if upsert_response['Errors']:
        #   subject = ''
        #   email_helpers.SendSocrataAlert(ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        #  log_payload = socrata_helpers.PrepPubLog(date_time, 'Signal Request Update', upsert_response)

        #  pub_log_response = socrata_helpers.UpsertData(SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        pdb.set_trace()

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)
