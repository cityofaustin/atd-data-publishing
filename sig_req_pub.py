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
    'FIELD_NAMES' : ['REQUEST_ID', 'ATD_LOCATION_ID', 'REQUEST_TYPE', 'FUNDING_STATUS', 'REQUEST_STATUS', 'PRIMARY_ST_SEGMENT_ID', 'CROSS_ST_SEGMENT_ID', 'GEOCODE', 'LANDMARK', 'PRIMARY_ST_AKA', 'CROSS_ST_AKA', 'SIGNAL_ENG_AREA', 'COUNCIL_DISTRICT', 'JURISDICTION', 'LOCATION_NAME', 'PRIMARY_ST', 'CROSS_ST', 'COA_INTERSECTION_ID', 'REQUEST_DATE', 'CROSS_ST_BLOCK', 'PRIMARY_ST_BLOCK', 'COUNTY'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

#  AGOL CONFIG
SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_signal_requests/FeatureServer/0/'

#  SOCRATA CONFIG
SOCRATA_RESOURCE_ID = ''
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'

#  CSV OUTPUT
CSV_DESTINATION = secrets.FME_DIRECTORY
DATASET_NAME = 'atd_signal_requests'

now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       

        field_list = knack_helpers.GetFields(KNACK_PARAMS)

        knack_data = knack_helpers.GetData(KNACK_PARAMS)

        knack_data = knack_helpers.ParseData(knack_data, field_list, KNACK_PARAMS, require_locations=True, convert_to_unix=True)

        knack_data = data_helpers.StringifyKeyValues(knack_data)
        
        knack_data_mills = data_helpers.ConvertUnixToMills(knack_data)

        token = agol_helpers.GetToken(secrets.AGOL_CREDENTIALS)

        agol_payload = agol_helpers.BuildPayload(knack_data_mills)

        del_response = agol_helpers.DeleteFeatures(SERVICE_URL, token)

        add_response = agol_helpers.AddFeatures(SERVICE_URL, token, agol_payload)

        #  write to csv
        knack_data = data_helpers.ConvertMillsToUnix(knack_data)
        knack_data = data_helpers.ConvertUnixToISO(knack_data)
        file_name = '{}/{}.csv'.format(CSV_DESTINATION, DATASET_NAME)
        data_helpers.WriteToCSV(knack_data, file_name=file_name)
        
        return add_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)
