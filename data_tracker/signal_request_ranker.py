'''
  assign traffic and PHB request rankings based on evaluation score
  dataset argument is required and must be either 'phb' or 'traffic_signal'

todo:

'''
import argparse
import logging
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
import traceback
from util import datautil
from util import emailutil


now = arrow.now()
now_s = now.format('YYYY_MM_DD')

logfile = '{}/sig_req_ranker_{}.log'.format( LOG_DIRECTORY, now_s )

logging.basicConfig(
    filename=logfile,
    level=logging.INFO
)

logging.info( 'START AT {}'.format(str(now)) )

primary_key = 'ATD_EVAL_ID'
status_key = 'EVAL_STATUS'
group_key = 'YR_MO_RND'
score_key = 'EVAL_SCORE'
concat_keys = ['RANK_ROUND_MO', 'RANK_ROUND_YR']
rank_key = 'EVAL_RANK'
status_vals = ['NEW', 'IN PROGRESS', 'COMPLETED']


eval_types = {
    'traffic_signal' : 'object_27', 
    'phb' : 'object_26'
}  

def main(date_time):

    try:    
        kn = knackpy.Knack(
            obj=eval_types[eval_type],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key']
        )

        #  filter data for only records in appropriate status
        data = datautil.filter_by_val(kn.data, status_key, status_vals)
        #  new records will not have a score key. add it here.
        data = datautil.add_missing_keys(data, { score_key : 0 } )
        #  create a ranking month_year field
        data = datautil.concat_key_values(data, concat_keys, group_key, '_')

        knack_data_exclude = [record for record in data if record['EXCLUDE_FROM_RANKING'] == True]
        knack_data_include = [record for record in data if record['EXCLUDE_FROM_RANKING'] == False]
    
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
            rank = datautil.min_index(score_dict[key], score) + 1  #  add one because list indices start at 0
            
            if rank_key in record:
                if record[rank_key] != rank:
                    record[rank_key] = rank
                    payload.append(record)

            else:
                record[rank_key] = rank

        #  assign null ranks to records flagged as exclude from ranking
        for record in knack_data_exclude:

            if rank_key in record:
                #  update excluded records if rank found
                if record[rank_key] != '':
                    record[rank_key] = ''
                    payload.append(record)
        
        if payload:
            #  parse data to core fields
            payload = datautil.reduce_to_keys(payload, [rank_key, 'id'])
            
            #  replace data keys with knack field names
            payload = datautil.replace_keys(payload, kn.field_map)
            
            update_response = []

            #  update knack records
            count = 0
            for record in payload:
                count += 1
                print( 'Updating record {} of {}'.format( 
                    count,
                    len(payload)
                ))

                logging.info( ' Updating record id {}'.format(
                    record['id']
                ))

                logging.info( 'Updating record {} of {}'.format(
                    count,
                    len(payload)
                ))

                response_json = knackpy.update_record(
                    record,
                    obj,
                    'id',
                    knack_creds['app_id'],
                    knack_creds['api_key']
                )


                update_response.append(response_json)

            return update_response
        else:
            logging.info(' No changes to upload.')
            return "No changes to upload."
        
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        logging.error( str(e) )
        error_text = traceback.format_exc()
        email_subject = "Signal Request Ranker Failure: {}".format(eval_type)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            email_subject,
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )

        print(e)
        raise e


def cli_args():
    """
    Parse command-line arguments using argparse module.
    """
    parser = argparse.ArgumentParser(
        prog='req_rank.py',
        description='Assign traffic and PHB request based on evaluation score.'
    )
    
    parser.add_argument(
        'eval_type',
        action="store",
        type=str,
        help='The type of evaluation score to rank: \'phb\' or \'traffic_signal\'.'
    )
    
    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        help='Name of the knack application that will be accessed. e.g. \'data_tracker_prod\''
    )

    args = parser.parse_args()

    return(args)


if __name__ == '__main__':
    
    args = cli_args()
    logging.info( 'args: {}'.format( str(args) ) )
    
    app_name = args.app_name
    knack_creds = KNACK_CREDENTIALS[app_name]
    eval_type = args.eval_type
    obj = eval_types[eval_type]

    results = main(now)
    
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

    print(results)









