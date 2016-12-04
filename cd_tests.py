
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
import secrets
import pdb

PRIMARY_KEY = 'ATD_SIGNAL_ID'

#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_12'],
    'SCENE' : '73',
    'VIEW' : '197',
    'FIELD_NAMES' : ['ATD_LOCATION_ID','ATD_SIGNAL_ID','COA_INTERSECTION_ID','CONTROL','COUNCIL_DISTRICT', 'CROSS_ST','CROSS_ST_AKA','CROSS_ST_SEGMENT_ID','JURISDICTION','LANDMARK','LOCATION_NAME','PRIMARY_ST', 'PRIMARY_ST_AKA','PRIMARY_ST_SEGMENT_ID','SIGNAL_ENG_AREA','SIGNAL_STATUS','SIGNAL_TYPE','TRAFFIC_ENG_AREA','MASTER_SIGNAL_ID', 'GEOCODE', 'IP_SWITCH', 'IP_CONTROL', 'SWITCH_COMM', 'COMM_PLAN', 'TURN_ON_DATE', 'MODIFIED_DATE'],
    'OUT_FIELDS' : ['ATD_LOCATION_ID','ATD_SIGNAL_ID','COA_INTERSECTION_ID','CONTROL','COUNCIL_DISTRICT', 'CROSS_ST','CROSS_ST_AKA','CROSS_ST_SEGMENT_ID','JURISDICTION','LANDMARK','LOCATION_NAME','PRIMARY_ST', 'PRIMARY_ST_AKA','PRIMARY_ST_SEGMENT_ID','SIGNAL_ENG_AREA','SIGNAL_STATUS','SIGNAL_TYPE','TRAFFIC_ENG_AREA','MASTER_SIGNAL_ID', 'LATITUDE', 'LONGITUDE', 'IP_SWITCH', 'IP_CONTROL', 'SWITCH_COMM', 'COMM_PLAN', 'TURN_ON_DATE', 'MODIFIED_DATE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
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

        field_list = {u'field_491_raw': {u'choices': [u'TURNED_ON', u'UKNOWN', u'CONSTRUCTION', u'REMOVED_PERMANENTLY', u'REMOVED_TEMPORARILY', u'UNDER_EVALUATION', u'APPROVED_FOR_CONST', u'DESIGN'], u'required': False, u'type': u'multiple_choice', u'key': u'field_491', u'label': u'SIGNAL_STATUS'}, u'field_181_raw': {u'required': False, u'type': u'number', u'key': u'field_181', u'label': u'COA_INTERSECTION_ID'}, u'field_508_raw': {u'required': False, u'type': u'date_time', u'key': u'field_508', u'label': u'MODIFIED_DATE'}, u'field_493_raw': {u'required': False, u'type': u'number', u'key': u'field_493', u'label': u'IP_CONTROL'}, u'field_208_raw': {u'choices': [u'MASTER', u'SLAVE', u'UNKNOWN'], u'required': False, u'type': u'multiple_choice', u'key': u'field_208', u'label': u'CONTROL'}, u'field_313_raw': {u'required': False, u'type': u'concatenation', u'key': u'field_313', u'label': u'PRIMARY_ST'}, u'field_190_raw': {u'choices': [u'AUSTIN FULL PURPOSE', u'AUSTIN 2 MILE ETJ', u'AUSTIN LTD', u'CEDAR PARK ETJ', u'HUTTO ETJ', u'ROLLINGWOOD FULL PURPOSE', u'SUNSET VALLEY FULL PURPOSE', u'WEST LAKE HILLS ETJ'], u'required': False, u'type': u'multiple_choice', u'key': u'field_190', u'label': u'JURISDICTION'}, u'field_205_raw': {u'required': False, u'type': u'date_time', u'key': u'field_205', u'label': u'MODIFIED_DATE'}, u'field_201_raw': {u'choices': [u'PHB', u'TRAFFIC'], u'required': False, u'type': u'multiple_choice', u'key': u'field_201', u'label': u'SIGNAL_TYPE'}, u'field_184_raw': {u'required': False, u'type': u'connection', u'relationship': {u'has': u'one', u'belongs_to': u'many', u'object': u'object_7'}, u'key': u'field_184', u'label': u'CROSS_ST_SEGMENT_ID'}, u'field_192_raw': {u'required': False, u'type': u'short_text', u'key': u'field_192', u'label': u'TRAFFIC_ENG_AREA'}, u'field_188_raw': {u'choices': [u'EAST', u'NORTH', u'SOUTH'], u'required': False, u'type': u'multiple_choice', u'key': u'field_188', u'label': u'SIGNAL_ENG_AREA'}, u'field_314_raw': {u'required': False, u'type': u'concatenation', u'key': u'field_314', u'label': u'CROSS_ST'}, u'field_185_raw': {u'required': False, u'type': u'short_text', u'key': u'field_185', u'label': u'LANDMARK'}, u'field_211_raw': {u'required': False, u'type': u'concatenation', u'key': u'field_211', u'label': u'LOCATION_NAME'}, u'field_187_raw': {u'required': False, u'type': u'short_text', u'key': u'field_187', u'label': u'CROSS_ST_AKA'}, u'field_186_raw': {u'required': False, u'type': u'short_text', u'key': u'field_186', u'label': u'PRIMARY_ST_AKA'}, u'field_182_raw': {u'required': False, u'type': u'address', u'key': u'field_182', u'label': u'GEOCODE'}, u'field_204_raw': {u'required': False, u'type': u'date_time', u'key': u'field_204', u'label': u'TURN_ON_DATE'}, u'field_183_raw': {u'required': False, u'type': u'connection', u'relationship': {u'has': u'one', u'belongs_to': u'many', u'object': u'object_7'}, u'key': u'field_183', u'label': u'PRIMARY_ST_SEGMENT_ID'}, u'field_189_raw': {u'required': False, u'type': u'number', u'key': u'field_189', u'label': u'COUNCIL_DISTRICT'}, u'field_199_raw': {u'required': False, u'type': u'number', u'key': u'field_199', u'label': u'ATD_SIGNAL_ID'}, u'field_212_raw': {u'required': False, u'type': u'concatenation', u'key': u'field_212', u'label': u'LOCATION_NAME'}, u'field_492_raw': {u'required': False, u'type': u'number', u'key': u'field_492', u'label': u'IP_SWITCH'}, u'field_494_raw': {u'choices': [u'Both', u'First Choice', u'N/A', u'None', u'Third Choice'], u'required': False, u'type': u'multiple_choice', u'key': u'field_494', u'label': u'SWITCH_COMM'}, u'field_200_raw': {u'required': False, u'type': u'short_text', u'key': u'field_200', u'label': u'MASTER_SIGNAL_ID'}, u'field_209_raw': {u'required': False, u'type': u'connection', u'relationship': {u'has': u'one', u'belongs_to': u'many', u'object': u'object_11'}, u'key': u'field_209', u'label': u'ATD_LOCATION_ID'}, u'field_180_raw': {u'required': False, u'type': u'number', u'key': u'field_180', u'label': u'ATD_LOCATION_ID'}}
        
        knack_data_raw = [{u'field_189': u'<span id="58164a4c407769ce2a14d093">1</span>', u'field_188': u'<span id="58164a4c407769ce2a14d093">NORTH</span>', u'field_181_raw': 5168452, u'field_491_raw': u'CONSTRUCTION', u'field_181': u'<span id="58164a4c407769ce2a14d093">5168452</span>', u'field_180': u'<span id="58164a4c407769ce2a14d093">1404</span>', u'field_183': u'<span id="58164a4c407769ce2a14d093"><span class="58164675838f38d62aa9072d">3501498</span></span>', u'field_182': u'<span id="58164a4c407769ce2a14d093">12937 Harris Branch Parkway<br />Manor, Texas 78653</span>', u'field_185': u'', u'field_184': u'<span id="58164a4c407769ce2a14d093"><span class="5816466e838f38d62aa90711">3263321</span></span>', u'field_187': u'', u'field_186': u'', u'id': u'5818b27c19b53944294fc6cb', u'field_314': u'<span id="58164a4c407769ce2a14d093">HARRIS BRANCH PKWY</span>', u'field_208_raw': u'MASTER', u'field_313': u'<span id="58164a4c407769ce2a14d093">E HOWARD LN</span>', u'field_313_raw': u'E HOWARD LN', u'field_190_raw': u'AUSTIN FULL PURPOSE', u'field_205_raw': {u'am_pm': u'AM', u'timestamp': u'11/01/2016 12:00 am', u'hours': u'12', u'time': 720, u'date': u'11/01/2016', u'unix_timestamp': 1477958400000, u'date_formatted': u'11/01/2016', u'minutes': u'00'}, u'field_208': u'MASTER', u'field_201_raw': u'TRAFFIC', u'field_494_raw': u'Both', u'field_184_raw': [{u'identifier': 3263321, u'id': u'5816466e838f38d62aa90711'}], u'field_201': u'TRAFFIC', u'field_204': u'11/01/2016', u'field_205': u'11/01/2016 12:00am', u'field_198': u'', u'field_199': u'387', u'field_188_raw': u'NORTH', u'field_190': u'<span id="58164a4c407769ce2a14d093">AUSTIN FULL PURPOSE</span>', u'field_314_raw': u'HARRIS BRANCH PKWY', u'field_185_raw': [], u'field_493': u'', u'field_492': u'', u'field_491': u'CONSTRUCTION', u'field_211_raw': u'E HOWARD LN / HARRIS BRANCH PKWY ', u'field_187_raw': [], u'field_494': u'Both', u'field_200': u'', u'field_186_raw': [], u'field_182_raw': {u'city': u'Manor', u'zip': u'78653', u'country': u'United States', u'formatted_value': u'12937 Harris Branch Parkway Manor, Texas 78653', u'longitude': u'-97.6084729', u'state': u'Texas', u'street': u'12937 Harris Branch Parkway', u'latitude': u'30.3780216'}, u'field_204_raw': {u'am_pm': u'AM', u'timestamp': u'11/01/2016 12:00 am', u'hours': u'12', u'time': 720, u'date': u'11/01/2016', u'unix_timestamp': 1477958400000, u'date_formatted': u'11/01/2016', u'minutes': u'00'}, u'field_183_raw': [{u'identifier': 3501498, u'id': u'58164675838f38d62aa9072d'}], u'field_189_raw': 1, u'field_199_raw': 387, u'field_211': u'<span id="58164a4c407769ce2a14d093">E HOWARD LN / HARRIS BRANCH PKWY </span>', u'field_200_raw': u'', u'field_180_raw': 1404}]
        
        knack_data = knack_helpers.ParseData(knack_data_raw, field_list, KNACK_PARAMS, require_locations=True, convert_to_unix=True)

        knack_data = data_helpers.StringifyKeyValues(knack_data)

        socrata_data = [{u'CONTROL': 'MASTER', u'LOCATION': "{u'latitude': u'30.3780216', u'needs_recoding': False, u'longitude': u'-97.6084729'}", u'COUNCIL_DISTRICT': '1', u'JURISDICTION': 'AUSTIN FULL PURPOSE', u'LOCATION_NAME': 'E HOWARD LN / HARRIS BRANCH PKWY ', u'SIGNAL_STATUS': 'CONSTRUCTION', u'PRIMARY_ST_SEGMENT_ID': '3501498', u'COA_INTERSECTION_ID': '5168452', u'CROSS_ST': 'HARRIS BRANCH PKWY', u'SIGNAL_TYPE': 'TRAFFIC', u'CROSS_ST_SEGMENT_ID': '3263321', u'PRIMARY_ST': 'E HOWARD LN', u'ATD_LOCATION_ID': '1404', u'SIGNAL_ENG_AREA': 'NORTH', u'SWITCH_COMM': 'Both', u'ATD_SIGNAL_ID': '387', 'MODIFIED_DATE': '1477958400', 'TURN_ON_DATE': 1477958400}]
        
        socrata_data = data_helpers.StringifyKeyValues(socrata_data)

        cd_results = data_helpers.DetectChanges(socrata_data, knack_data, PRIMARY_KEY, keys=KNACK_PARAMS['FIELD_NAMES'])

        for key in cd_results:
            print('{} : {}'.format(key, len(cd_results[key])))

        pdb.set_trace()

        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            socrata_payload = socrata_helpers.CreatePayload(cd_results, PRIMARY_KEY)

            socrata_payload = socrata_helpers.CreateLocationFields(socrata_payload)

        else:
            socrata_payload = []

        socrata_payload = data_helpers.LowerCaseKeys(socrata_payload)

        socrata_payload = data_helpers.ConvertUnixToISO(socrata_payload)
        
        upsert_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_RESOURCE_ID)

        pdb.set_trace()

        if 'error' in upsert_response:
            email_helpers.SendSocrataAlert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)
            
        elif upsert_response['Errors']:
            email_helpers.SendSocrataAlert(secrets.ALERTS_DISTRIBUTION, SOCRATA_RESOURCE_ID, upsert_response)

        log_payload = socrata_helpers.PrepPubLog(date_time, 'signals_update', upsert_response)

        pub_log_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)

        return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)


