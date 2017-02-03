
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
    'REFERENCE_OBJECTS' : ['object_13', 'object_26'],
    'SCENE' : '175',
    'VIEW' : '908',
    'FIELD_NAMES' : ['REQUEST_ID', 'REQUEST_TYPER', 'REQUEST_STATUS', 'ATD_EVAL_ID', 'EVAL_STATUS', 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'LOCATION_NAME', 'LANDMARK', 'PRIMARY_ST_BLOCK', 'PRIMARY_ST_PREFIX_DIRECTION', 'PRIMARY_ST', 'CROSS_ST_BLOCK', 'CROSS_ST_PREFIX_DIRECTION', 'CROSS_ST', 'FUNDING_STATUS', 'EVAL_SCORE', 'EVAL_RANK'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}


now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       

        field_list = knack_helpers.GetFields(KNACK_PARAMS_TRAFFIC)

        knack_data = knack_helpers.GetData(KNACK_PARAMS_TRAFFIC)

        pdb.set_trace()

        knack_data = knack_helpers.ParseData(knack_data, field_list, KNACK_PARAMS_TRAFFIC, require_locations=True, convert_to_unix=True)    

        knack_data = data_helpers.StringifyKeyValues(knack_data)
        
        knack_data_mills = data_helpers.ConvertUnixToMills(deepcopy(knack_data))

        pdb.set_trace()

        
        return "hello"
        # return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


results = main(now)

print(results)







