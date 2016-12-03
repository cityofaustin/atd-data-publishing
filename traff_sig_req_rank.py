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

PRIMARY_KEY = 'ATD_EVAL_ID'
STATUS_KEY = 'TRAFFIC_EVAL_STATUS'
GROUP_KEY = 'YR_MO_RND'
SCORE_KEY = 'EVAL_SCORE'

KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_27'],
    'SCENE' : '175',
    'VIEW' : '508',
    'FIELD_NAMES' : ['ATD_EVAL_ID', 'TRAFFIC_EVAL_STATUS', 'EVAL_SCORE', 'RANK_ROUND_MO', 'RANK_ROUND_YR'],
    'OUT_FIELDS' : ['ATD_EVAL_ID', 'TRAFFIC_EVAL_STATUS', 'EVAL_SCORE', 'RANK_ROUND_MO', 'RANK_ROUND_YR'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

KNACK_PARAMS_2 = {  
    'REFERENCE_OBJECTS' : ['object_27'],
    'FIELD_NAMES' : ['ATD_EVAL_ID'],
    'OUT_FIELDS' : ['ATD_EVAL_ID'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

CONCAT_KEYS = ['RANK_ROUND_MO', 'RANK_ROUND_YR']

now = arrow.now()

def main(date_time):
    print('starting stuff now')

    try:       
        field_list = knack_helpers.GetFields(KNACK_PARAMS_2)

        id_data = knack_helpers.GetObjectData(KNACK_PARAMS_2)

        id_data = knack_helpers.ParseData(id_data, field_list, KNACK_PARAMS_2, include_ids=True)

        field_list = knack_helpers.GetFields(KNACK_PARAMS)

        source_data = knack_helpers.GetData(KNACK_PARAMS)

        source_data = knack_helpers.ParseData(source_data, field_list, KNACK_PARAMS)

        knack_data = data_helpers.StringifyKeyValues(source_data)

        knack_data = data_helpers.FilterbyKey(knack_data, STATUS_KEY, ['NEW', 'IN PROGRESS', 'COMPLETED'])
        
        knack_data = data_helpers.FilterbyKeyExists(knack_data, SCORE_KEY)

        knack_data = data_helpers.ConcatKeyVals(knack_data, CONCAT_KEYS, GROUP_KEY, '_')
        
        knack_data_dict = data_helpers.GroupByUniqueValue(knack_data, GROUP_KEY)
        
        for d in knack_data_dict:
            knack_data_dict[d] = data_helpers.SortDictsInt(knack_data_dict[d], SCORE_KEY)


        for d in knack_data_dict:
            knack_data_dict[d] = data_helpers.createRankList(knack_data_dict[d])

        for d in knack_data_dict:
            knack_data_dict[d] = data_helpers.ReduceDicts(knack_data_dict[d], [PRIMARY_KEY, 'RANK', 'EVAL_SCORE'])

        ranked_data = []

        for key in knack_data_dict:
            for d in knack_data_dict[key]: 
                ranked_data.append(d)

        final_data = data_helpers.MergeDicts(ranked_data, id_data, PRIMARY_KEY, ['knack_id'])

        return final_data
        
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


r = main(now)


