'''
check traffic signal prevent maintenance (PM) records and
insert copies of PM records to signals' secondary signals
'''
import argparse
import logging
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import datautil
from util import emailutil


def get_prim_signals(list_of_signals):
    '''
    create a dict of primary signals with and the secondary signals they control
    expects list_of_signals to have 'KNACK_ID', i.e knack data has been parsed
    with option include_ids=True
    '''
    signals_with_children = {}

    for signal in list_of_signals:
        if 'SECONDARY_SIGNALS' in signal:
            if len(signal['SECONDARY_SIGNALS']) > 0:
                signals_with_children[signal['id']] = signal['SECONDARY_SIGNALS']
                    
    return signals_with_children


def copy_pm_record(destination_signal_id, source_record, copy_fields):
    
    new_record = {
        'SIGNAL' : [destination_signal_id],
        'COPIED_FROM_ID' : source_record['ATD_PM_ID'],
        'PM_STATUS': 'COMPLETED',
        'COPIED_FROM_PRIMARY' : True,
        'COPIED_TO_SECONDARY' : False
    }

    for field in copy_fields:
        if field in source_record:
            new_record[field] = source_record[field]

    return new_record


def cli_args():
    parser = argparse.ArgumentParser(
        prog='signal_pm_copier.py',
        description='''
            check traffic signal prevent maintenance (PM) records and
            insert copies of PM records to signals' secondary signals
        '''
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        help='Name of the knack application that will be accessed'
    )

    args = parser.parse_args()    
    return(args)


def main(date_time):
    print('starting stuff now')

    try:
        #  get preventative maintenance (pm) records
        knack_data_pm = knackpy.Knack(
            view=params_pm['view'],
            scene=params_pm['scene'],
            ref_obj=params_pm['field_obj'],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            raw_connections=True
        )
        
        data_pm = []
        
        for pm in knack_data_pm.data:
            #  verify there is data that needs to be processed
            if (not pm['COPIED_TO_SECONDARY'] and
                pm['PM_STATUS'] == 'COMPLETED' and
                int(pm['SECONDARY_SIGNALS_COUNT']) > 0):
                
                data_pm.append(pm)

        if not data_pm:
            logging.info('No PM records to copy.')
            return

        #  get signal data
        knack_data_signals = knackpy.Knack(
            view=params_signal['view'],
            scene=params_signal['scene'],
            ref_obj=params_signal['field_obj'],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            raw_connections=True
        )

        primary_signals_with_children = get_prim_signals(knack_data_signals.data)

        payload_insert = []
        payload_update = []
        
        for pm in data_pm:
            '''
            check all preventative maintenance records at signals with secondary signals
            copy pm record to secondary signal if needed
            '''
            if 'SIGNAL' in pm:
                
                primary_signal_id = pm['SIGNAL'][0]['id']

                if primary_signal_id in primary_signals_with_children:
                    #  update original pm record with copied to secondary = True
                    payload_update.append({
                        'id' : pm['id'],
                        'COPIED_TO_SECONDARY' : True
                    })

                    for secondary in primary_signals_with_children[primary_signal_id]:
                        #  create new pm record for secondary signal(s)
                        new_record = copy_pm_record(
                            secondary['id'], pm,
                            copy_fields
                        )

                        payload_insert.append(new_record)
    
        payload_update = datautil.replace_keys(
            payload_update,
            knack_data_pm.field_map
        )

        payload_insert = datautil.replace_keys(
            payload_insert,
            knack_data_pm.field_map
        )

        count = 0
        update_response = []
        
        for record in payload_update:
            count += 1
            print( 'update record {} of {}'.format( count, len(payload_insert) ) )
            logging.info('update record {} of {}'.format( count, len(payload_insert) ) )
            
            response_json = knackpy.update_record(
                record,
                params_pm['field_obj'][0],
                'id',
                knack_creds['app_id'],
                knack_creds['api_key']
            )

            logging.info(response_json)
            update_response.append(response_json)

        count = 0

        for record in payload_insert:
            count += 1
            print( 'insert record {} of {}'.format( count, len(payload_insert) ) )
            logging.info('insert record {} of {}'.format( count, len(payload_insert) ) )
            
            response_json = knackpy.insert_record(
                record,
                params_pm['field_obj'][0],
                knack_creds['app_id'],
                knack_creds['api_key']
            )

            logging.info(response_json)
            update_response.append(response_json)

        logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
        return "done"

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        emailutil.send_email(ALERTS_DISTRIBUTION, 'Copy Preventative Maintenance Failure', str(e), EMAIL['user'], EMAIL['password'])
        logging.error( str(e) )

        print(e)
        raise e


if __name__ == '__main__':
    #  parse command-line arguments
    args = cli_args()
    app_name = args.app_name

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    logfile = '{}/sig_pm_copier_{}.log'.format(LOG_DIRECTORY, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    knack_creds = KNACK_CREDENTIALS[app_name]

    params_pm = {  
        'field_obj' : ['object_84', 'object_12'],
        'scene' : 'scene_416',
        'view' : 'view_1182'
    }

    params_signal = {  
        'field_obj' : ['object_12'],
        'scene' : 'scene_73',
        'view' : 'view_197'
    }

    copy_fields = ['PM_COMPLETED_DATE', 'WORK_ORDER', 'PM_COMPLETED_BY']

    results = main(now)
    print(results)    







