#  assign traffic and PHB request rankings based on evaluation score
#  dataset argument is required and must be either 'phb' or 'traffic_signal'

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import argparse
import logging
import arrow
import knack_helpers
import data_helpers
import email_helpers
import secrets

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

logfile = './log/sig_req_ranker_{}.log'.format(now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

primary_key = 'ATD_EVAL_ID'
status_key = 'EVAL_STATUS'
group_key = 'YR_MO_RND'
score_key = 'EVAL_SCORE'
concat_keys = ['RANK_ROUND_MO', 'RANK_ROUND_YR']
rank_key = 'EVAL_RANK'
field_names = [primary_key, rank_key, status_key, score_key, 'RANK_ROUND_MO', 'RANK_ROUND_YR', 'EXCLUDE_FROM_RANKING']

knack_creds = secrets.KNACK_CREDENTIALS

eval_types = {
    'traffic_signal' : ['object_27'], 
    'phb' : ['object_26']
}  



def main(date_time):

    try:    
        field_data = knack_helpers.get_fields( objects, knack_creds )
        field_lookup = knack_helpers.create_field_lookup(field_data, parse_raw=True)
        knack_data = knack_helpers.get_object_data( objects[0], knack_creds )
        knack_data = knack_helpers.parse_data(knack_data, field_data, include_ids=True)
        knack_data = data_helpers.filter_by_key(knack_data, status_key, ['NEW', 'IN PROGRESS', 'COMPLETED'])
        knack_data = data_helpers.add_missing_keys(knack_data, [score_key], ['0'])
        knack_data = data_helpers.concat_key_values(knack_data, concat_keys, group_key, '_')
        knack_data_exclude = [record for record in knack_data if record['EXCLUDE_FROM_RANKING'] == True]
        knack_data_include = [record for record in knack_data if record['EXCLUDE_FROM_RANKING'] == False]
    
        #  create list of scores grouped by group key
        #  scores are converted to integers
        score_dict = {}

        for row in knack_data_include:
            key = row[group_key]
            score = int( row[score_key] )

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
            score = int( record[score_key] )
            key = record[group_key]
            rank = data_helpers.min_index(score_dict[key], score) + 1  #  add one to score index, because list indices start at 0
            
            if rank_key in record:
                if record[rank_key] != rank:
                    record[rank_key] = rank
                    payload.append(record)

            else:
                record[rank_key] = rank

        #  assign null ranks to records flagged as exclude from ranking
        for record in knack_data_exclude:

            if rank_key in record:
                #  updated excluded records if rank found
                if record[rank_key] != '':
                    record[rank_key] = ''
                    payload.append(record)
        
        if payload:
            #  parse data to core fields
            payload = data_helpers.reduce_dicts(payload, [rank_key, 'KNACK_ID'])
            
            #  replace data keys with knack field names
            payload = data_helpers.replace_keys(payload, field_lookup)
            
            update_response = []

            #  update knack records
            count = 0
            for record in payload:
                count += 1
                print( 'Updating record {} of {}'.format( count, len(payload) ) )
                logging.info( ' Updating record id {}'.format(record['KNACK_ID']) )
                logging.info( 'Updating record {} of {}'.format( count, len(payload) ))
                response_json = knack_helpers.update_record(record, objects[0], 'KNACK_ID', knack_creds)

                update_response.append(response_json)

            return update_response
        else:
            logging.info(' No changes to upload.')
            return "No changes to upload."
        
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        logging.error( str(e) )
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Signal Request Rank Update Failure', str(e))
        print(e)
        raise e


def cli_args():
    """
    Parse command-line arguments using argparse module.
    """
    parser = argparse.ArgumentParser(prog='req_rank.py', description='Assign traffic and PHB request based on evaluation score.')
    parser.add_argument('eval_type', action="store", type=str, help='The type of evaluation score to rank: \'phb\' or \'traffic_signal\'.')
    args = parser.parse_args()
    return(args)





if __name__ == '__main__':
    
    args = cli_args()
    logging.info( 'args: {}'.format( str(args) ) )
    
    eval_type = args.eval_type
    objects = eval_types[eval_type]
    
    results = main(now)
    
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

    print(results)