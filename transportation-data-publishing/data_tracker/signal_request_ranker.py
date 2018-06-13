'''
Assign traffic and PHB request rankings based on evaluation score
dataset argument is required and must be either 'phb' or 'traffic_signal'
'''
import argparse
import os
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

primary_key = 'ATD_EVAL_ID'
status_key = 'EVAL_STATUS'
group_key = 'YR_MO_RND'
score_key = 'EVAL_SCORE'
concat_keys = ['RANK_ROUND_MO', 'RANK_ROUND_YR']
rank_key = 'EVAL_RANK'
status_vals = ['NEW', 'IN PROGRESS', 'COMPLETED']
modified_date_key = 'MODIFIED_DATE'


eval_types = {
    'traffic_signal' : 'object_27', 
    'phb' : 'object_26'
}  


def main():
    kn = knackpy.Knack(
        obj=eval_types[eval_type],
        app_id=knack_creds['app_id'],
        api_key=knack_creds['api_key']
    )

    data = datautil.filter_by_val(kn.data, status_key, status_vals)
    
    #  new records will not have a score key. add it here.
    data = datautil.add_missing_keys(data, { score_key : 0 } )
    
    #  create a ranking month_year field
    data = datautil.concat_key_values(data, concat_keys, group_key, '_')

    knack_data_exclude = [record for record in data if record['EXCLUDE_FROM_RANKING'] == True]
    knack_data_include = [record for record in data if record['EXCLUDE_FROM_RANKING'] == False]

    #  create list of scores grouped by group key
    score_dict = {}

    for row in knack_data_include:
        key = row[group_key]
        score = int( row[score_key] )

        if key not in score_dict:
            score_dict[key] = []

        score_dict[key].append(score)

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
                record[modified_date_key] = datautil.local_timestamp()
                payload.append(record)

        else:
            record[rank_key] = rank

    #  assign null ranks to records flagged as exclude from ranking
    for record in knack_data_exclude:

        if rank_key in record:
            #  update excluded records if rank found
            if record[rank_key] != '':
                record[rank_key] = ''
                record[modified_date_key] = datautil.local_timestamp()
                payload.append(record)
    
    if payload:

        payload = datautil.reduce_to_keys(payload, [rank_key, 'id', modified_date_key])
        
        payload = datautil.replace_keys(payload, kn.field_map)
        
        update_response = []

        count = 0
        for record in payload:
            count += 1

            print( 'Updating record {} of {}'.format( 
                count,
                len(payload)
            ))

            res = knackpy.record(
                record,
                obj_key=obj,
                app_id= knack_creds['app_id'],
                api_key= knack_creds['api_key'],
                method='update',
            )

            update_response.append(res)

        return len(payload)
    
    else:
        return 0
    

def cli_args():
    '''
    Parse command-line arguments using argparse module.
    '''
    parser = argutil.get_parser(
        'signal_requests_ranker.py',
        'Assign traffic and PHB request based on evaluation score.',
        'eval_type',
        'app_name',
    )
    
    args = parser.parse_args()
    
    return args


if __name__ == '__main__':
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'

    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    args = cli_args()
    logger.info( 'args: {}'.format( str(args) ) )
    
    app_name = args.app_name
    knack_creds = KNACK_CREDENTIALS[app_name]
    eval_type = args.eval_type
    obj = eval_types[eval_type]

    script_id = f'{script_name}_{eval_type}'

    try:
        job = jobutil.Job(
            name=script_id,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)
        
        job.start()

        results = main()
        
        job.result('success', records_processed=results)

        logger.info('END AT {}'.format( arrow.now() ))
        
    except Exception as e:
        logger.error( str(e) )
        error_text = traceback.format_exc()
        email_subject = "Signal Request Ranker Failure: {}".format(eval_type)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            email_subject,
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )

        job.result('error', message=str(e))

        raise e









