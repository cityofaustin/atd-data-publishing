
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
KNACK_PARAMS_TRAFFIC = {  
    'REFERENCE_OBJECTS' : ['object_13', 'object_27'],
    'SCENE' : '175',
    'VIEW' : '908',
    'FIELD_NAMES' : ['REQUEST_ID', 'EVAL_TYPE', 'REQUEST_STATUS', 'ATD_EVAL_ID', 'EVAL_STATUS', 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'LOCATION_NAME', 'LANDMARK', 'PRIMARY_ST_BLOCK', 'PRIMARY_ST_PREFIX_DIRECTION', 'PRIMARY_ST', 'CROSS_ST_BLOCK', 'CROSS_ST_PREFIX_DIRECTION', 'CROSS_ST', 'FUNDING_STATUS', 'EVAL_SCORE', 'EVAL_RANK'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

KNACK_PARAMS_PHB = {  
    'REFERENCE_OBJECTS' : ['object_13', 'object_26'],
    'SCENE' : '175',
    'VIEW' : '911',
    'FIELD_NAMES' : ['REQUEST_ID', 'EVAL_TYPE', 'REQUEST_STATUS', 'ATD_EVAL_ID', 'EVAL_STATUS', 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'LOCATION_NAME', 'LANDMARK', 'PRIMARY_ST_BLOCK', 'PRIMARY_ST_PREFIX_DIRECTION', 'PRIMARY_ST', 'CROSS_ST_BLOCK', 'CROSS_ST_PREFIX_DIRECTION', 'CROSS_ST', 'FUNDING_STATUS', 'EVAL_SCORE', 'EVAL_RANK'],
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

#  CSV OUTPUT
CSV_DESTINATION = secrets.FME_DIRECTORY
DATASET_NAME = 'atd_signals'

now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       
        #  get and parse phb eval data
        field_list = knack_helpers.GetFields(KNACK_PARAMS_PHB)

        knack_data_phb = knack_helpers.GetData(KNACK_PARAMS_PHB)

        knack_data_phb = knack_helpers.ParseData(knack_data_phb, field_list, KNACK_PARAMS_TRAFFIC, convert_to_unix=True)

        knack_data_phb = data_helpers.StringifyKeyValues(knack_data_phb)
        
        knack_data_phb_mills = data_helpers.ConvertUnixToMills(deepcopy(knack_data_phb))

        #  get and parse traffic eval data
        field_list = knack_helpers.GetFields(KNACK_PARAMS_TRAFFIC)

        knack_data_traffic = knack_helpers.GetData(KNACK_PARAMS_TRAFFIC)

        knack_data_traffic = knack_helpers.ParseData(knack_data_traffic, field_list, KNACK_PARAMS_TRAFFIC, convert_to_unix=True)

        knack_data_traffic = data_helpers.StringifyKeyValues(knack_data_traffic)
        
        knack_data_traffic_mills = data_helpers.ConvertUnixToMills(deepcopy(knack_data_traffic))
        

        knack_data_master = knack_data_traffic_mills + knack_data_phb_mills
        
        #  get and parse location info
        field_list = knack_helpers.GetFields(KNACK_PARAMS_REQ_LOCATIONS)

        knack_data_req_loc = knack_helpers.GetData(KNACK_PARAMS_REQ_LOCATIONS)

        knack_data_req_loc = knack_helpers.ParseData(knack_data_req_loc, field_list, KNACK_PARAMS_REQ_LOCATIONS, convert_to_unix=True)

        knack_data_req_loc = data_helpers.StringifyKeyValues(knack_data_req_loc)

        #  append location info to eval data dicts
        knack_data_master =data_helpers.MergeDicts(knack_data_master, knack_data_req_loc, 'REQUEST_ID', ['LATITUDE', 'LONGITUDE'])

        file_name = '{}/{}.csv'.format(CSV_DESTINATION, DATASET_NAME)
        data_helpers.WriteToCSV(knack_data_master, file_name=file_name)

        pdb.set_trace()



        return "hello"
        # return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)
