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


#  KNACK CONFIG
REFERENCE_OBJECT = 'object_27'
PRIMARY_KEY = 'ATD_EVAL_ID'
STATUS_KEY = 'TRAFFIC_EVAL_STATUS'
GROUP_KEY = 'YR_MO_RND'
SCORE_KEY = 'EVAL_SCORE'
CONCAT_KEYS = ['RANK_ROUND_MO', 'RANK_ROUND_YR']
RANK_KEY = 'TRAFFIC_EVAL_RANK'

KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : [REFERENCE_OBJECT],
    'SCENE' : '175',
    'VIEW' : '508',
    'FIELD_NAMES' : [PRIMARY_KEY, 'TRAFFIC_EVAL_STATUS', 'EVAL_SCORE', 'RANK_ROUND_MO', 'RANK_ROUND_YR'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

KNACK_PARAMS_2 = {  
    'REFERENCE_OBJECTS' : [REFERENCE_OBJECT],
    'FIELD_NAMES' : [PRIMARY_KEY, RANK_KEY],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

now = arrow.now()

def main(date_time):

    try:       
        field_dict = knack_helpers.GetFields( KNACK_PARAMS_2 )

        field_lookup = knack_helpers.CreateFieldLookup(field_dict, parse_raw=True)

        id_data = knack_helpers.GetObjectData( REFERENCE_OBJECT, KNACK_PARAMS_2 )

        id_data = knack_helpers.ParseData(id_data, field_dict, KNACK_PARAMS_2, include_ids=True)

        field_dict = knack_helpers.GetFields(KNACK_PARAMS)

        source_data = knack_helpers.GetData(KNACK_PARAMS)

        source_data = knack_helpers.ParseData(source_data, field_dict, KNACK_PARAMS)

        knack_data = data_helpers.StringifyKeyValues(source_data)

        knack_data = data_helpers.FilterbyKey(knack_data, STATUS_KEY, ['NEW', 'IN PROGRESS', 'COMPLETED'])
        
        knack_data = data_helpers.AddMissingKeys(knack_data, [SCORE_KEY], '0')
        
        knack_data = data_helpers.FilterbyKeyExists(knack_data, SCORE_KEY)
        
        knack_data = data_helpers.ConcatKeyVals(knack_data, CONCAT_KEYS, GROUP_KEY, '_')
        
        knack_data_dict = data_helpers.GroupByUniqueValue(knack_data, GROUP_KEY)
        
        for d in knack_data_dict:
            knack_data_dict[d] = data_helpers.SortDictsInt(knack_data_dict[d], SCORE_KEY)

        for d in knack_data_dict:
            knack_data_dict[d] = data_helpers.createRankList(knack_data_dict[d], RANK_KEY)

        ranked_data = []

        for key in knack_data_dict:
            for d in knack_data_dict[key]: 
                ranked_data.append(d)

        ranked_data = data_helpers.MergeDicts(ranked_data, id_data, PRIMARY_KEY, ['KNACK_ID'])

        ranked_data = data_helpers.ReduceDicts(ranked_data, [RANK_KEY, 'KNACK_ID'])

        ranked_data = data_helpers.ReplaceDictKeys(ranked_data, field_lookup)

        pdb.set_trace()

        update_response = []

        for record in ranked_data:
            response_json = knack_helpers.UpdateRecord(record, KNACK_PARAMS)
            update_response.append(response_json)

        return update_response
        
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


r = main(now)

print(r)


