if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
import knack_helpers
import data_helpers
import secrets
import pdb


#  KNACK CONFIG
REFERENCE_OBJECT = 'object_27'
PRIMARY_KEY = 'ATD_EVAL_ID'
STATUS_KEY = 'EVAL_STATUS'
GROUP_KEY = 'YR_MO_RND'
SCORE_KEY = 'EVAL_SCORE'
CONCAT_KEYS = ['RANK_ROUND_MO', 'RANK_ROUND_YR']
RANK_KEY = 'EVAL_RANK'

KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : [REFERENCE_OBJECT],
    'FIELD_NAMES' : [PRIMARY_KEY, RANK_KEY, STATUS_KEY, SCORE_KEY, 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'EXCLUDE_FROM_RANKING'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

now = arrow.now()

def main(date_time):

    try:       
        field_dict = knack_helpers.get_fields( KNACK_PARAMS )

        field_lookup = knack_helpers.create_field_lookup(field_dict, parse_raw=True)

        knack_data = knack_helpers.get_object_data( REFERENCE_OBJECT, KNACK_PARAMS )

        knack_data = knack_helpers.parse_data(knack_data, field_dict, KNACK_PARAMS, include_ids=True)

        knack_data = data_helpers.filter_by_key(knack_data, STATUS_KEY, ['NEW', 'IN PROGRESS', 'COMPLETED'])

        knack_data = data_helpers.add_missing_keys(knack_data, [SCORE_KEY], ['0'])

        knack_data = data_helpers.concat_key_values(knack_data, CONCAT_KEYS, GROUP_KEY, '_')
        
        knack_data_exclude = [record for record in knack_data if record['EXCLUDE_FROM_RANKING'] == True]

        knack_data_include = [record for record in knack_data if record['EXCLUDE_FROM_RANKING'] == False]

        #  create list of scores grouped by group key
        #  scores are converted to integers
        score_dict = {}

        for row in knack_data_include:
            key = row[GROUP_KEY]
            score = int( row[SCORE_KEY] )

            if key not in score_dict:
                score_dict[key] = []

            score_dict[key].append(score)

        #  reverse sort lists of scores
        for key in score_dict:
            score_dict[key].sort()
            score_dict[key].reverse()

        #  get score rank and append record to payload
        payload = []

        for record in knack_data_include:
            score = int( record[SCORE_KEY] )
            key = record[GROUP_KEY]
            rank = data_helpers.min_index(score_dict[key], score) + 1  #  add one to score index, because list indices start at 0
            
            if RANK_KEY in record:
                if record[RANK_KEY] != rank:
                    record[RANK_KEY] = rank
                    payload.append(record)

            else:
                record[RANK_KEY] = rank
                payload.append(record)

        #  assign null ranks to records flagged as exclude from ranking
        for record in knack_data_exclude:

            if RANK_KEY in record:
                #  updated excluded records if rank found
                if record[RANK_KEY] != '':
                    record[RANK_KEY] = ''
                    payload.append(record)

        #  parse data to core fields
        payload = data_helpers.reduce_dicts(payload, [RANK_KEY, 'KNACK_ID'])

        #  replace data keys with knack field names
        payload = data_helpers.replace_keys(payload, field_lookup)

        update_response = []

        #  update knack records
        count = 0
        for record in payload:
            count += 1
            print( 'updating record {} of {}'.format( count, len(payload) ) )
            
            response_json = knack_helpers.update_record(record, KNACK_PARAMS)

            update_response.append(response_json)

        return update_response
        
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


r = main(now)

print('Donezo!')