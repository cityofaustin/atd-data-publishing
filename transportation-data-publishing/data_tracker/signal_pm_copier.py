'''
Check traffic signal prevent maintenance (PM) records and
insert copies of PM records to signals' secondary signals.
'''
import argparse
import os
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

params_pm = {
    'field_obj': ['object_84', 'object_12'],
    'scene': 'scene_416',
    'view': 'view_1182'
}

params_signal = {
    'field_obj': ['object_12'],
    'scene': 'scene_73',
    'view': 'view_197'
}

copy_fields = ['PM_COMPLETED_DATE', 'WORK_ORDER', 'PM_COMPLETED_BY']

def get_prim_signals(list_of_signals):
    '''
    Create a dict of primary signals with and the secondary signals they control.
    List_of_signals must have a Knack datbase id.
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


def apply_modified_date(dicts, key='MODIFIED_DATE', offset=600000):
    #  set the record modified date as a "local" timestamp (knack-friendly)
    #  also apply a forward offset to ensure modified records are picked up
    #  by the publishing scripts which are checking for recently modded data
    for record in dicts:
        record[key] = datautil.local_timestamp() + offset
    
    return dicts


def cli_args():
    parser = argutil.get_parser(
        'signal_pm_copier.py',
        'Check traffic signal prevent maintenance (PM) records and insert copies of PM records to signals\' secondary signals',
        'app_name',
    )
    
    args = parser.parse_args()
    
    return args


def main(job, **kwargs):

    app_name = kwargs["app_name"]

    knack_creds = KNACK_CREDENTIALS[app_name]



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

    pdb.set_trace()

    if not knack_data_pm.data:
    # logger.info('No PM records to copy.')
        return 0
    
    for pm in knack_data_pm.data:
        #  verify there is data that needs to be processed
        #  the source view is also filtered by these conditions
        #  so this is a redundant check, to be safe
        if (not pm['COPIED_TO_SECONDARY'] and
            pm['PM_STATUS'] == 'COMPLETED' and
            int(pm['SECONDARY_SIGNALS_COUNT']) > 0):
            
            data_pm.append(pm)

    if not data_pm:
        # logger.info('No PM records to copy.')
        return 0

    #  get signal data
    #  TODO: filter for signals and their secondaries based on PM data
    #  this would reduce size of request
    knack_data_signals = knackpy.Knack(
        view=params_signal['view'],
        scene=params_signal['scene'],
        ref_obj=params_signal['field_obj'],
        app_id=knack_creds['app_id'],
        api_key=knack_creds['api_key'],
        raw_connections=True
    )

    primary_signals_with_children = get_prim_signals(knack_data_signals.data)

    pm_payload_insert = []
    pm_payload_update = []
    signals_update = []

    for pm in data_pm:
        '''
        Check all preventative maintenance records at signals with secondary signals
        Copy pm record to secondary signal if needed
        '''
        if 'SIGNAL' in pm:
            
            primary_signal_id = pm['SIGNAL'][0]['id']

            if primary_signal_id in primary_signals_with_children:
                #  update original pm record with copied to secondary = True
                pm_payload_update.append({
                    'id' : pm['id'],
                    'COPIED_TO_SECONDARY' : True
                })

                for secondary in primary_signals_with_children[primary_signal_id]:
                    #  create new pm record for secondary signal(s)
                    new_record = copy_pm_record(
                        secondary['id'], pm,
                        copy_fields
                    )

                    signals_update.append( {'id' : secondary['id']})
                    pm_payload_insert.append(new_record)

    # update modified date of secondary signals which have a new PM
    signals_payload_update = apply_modified_date(signals_update)

    signals_payload_update = datautil.replace_keys(
        signals_payload_update,
        knack_data_signals.field_map
    )

    pm_payload_update = datautil.replace_keys(
        pm_payload_update,
        knack_data_pm.field_map
    )

    pm_payload_insert = datautil.replace_keys(
        pm_payload_insert,
        knack_data_pm.field_map
    )

    count = 0
    update_response = []
    
    for record in payload_update:
        count += 1
        print( 'update record {} of {}'.format( count, len(payload_insert) ) )
        # logger.info('update record {} of {}'.format( count,
        # len(payload_insert) ) )

    for record in signals_payload_update:
        res = knackpy.record(
            record,
            obj_key=params_signal['field_obj'][0],
            app_id= knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            method='update',
        )


        # logger.info(res)
        update_response.append(res)

    count = 0

    for record in payload_insert:
        count += 1
        print( 'insert record {} of {}'.format( count, len(payload_insert) ) )
        # logger.info('insert record {} of {}'.format( count,
        # len(payload_insert) ) )
        

    for record in pm_payload_update:
        res = knackpy.record(
            record,
            obj_key=params_pm['field_obj'][0],
            app_id= knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            method='update',
        )

    for record in pm_payload_insert:

        res = knackpy.record(
            record,
            obj_key=params_pm['field_obj'][0],
            app_id= knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            method='create',
        )

        # logger.info(res)
        update_response.append(res)

    # logger.info('END AT {}'.format( arrow.now() ))

    # logger.info('END AT {}'.format( arrow.now() ))

    
    return len(pm_payload_insert) + len(pm_payload_update) + len(signals_payload_update)



if __name__ == '__main__':
    # script_name = os.path.basename(__file__).replace('.py', '')
    # logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    #
    # logger = logutil.timed_rotating_log(logfile)
    # logger.info('START AT {}'.format( arrow.now() ))

    # args = cli_args()
    # app_name = args.app_name
    #
    # knack_creds = KNACK_CREDENTIALS[app_name]

    # params_pm = {
    #     'field_obj' : ['object_84', 'object_12'],
    #     'scene' : 'scene_416',
    #     'view' : 'view_1182'
    # }
    #
    # params_signal = {
    #     'field_obj' : ['object_12'],
    #     'scene' : 'scene_73',
    #     'view' : 'view_197'
    # }
    #
    # copy_fields = ['PM_COMPLETED_DATE', 'WORK_ORDER', 'PM_COMPLETED_BY']

    try:
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)
        
        job.start()

        results = main()

        job.result('success', records_processed=results)


    except Exception as e:
        logger.error( str(e) )

        emailutil.send_email(ALERTS_DISTRIBUTION, 'Copy Preventative Maintenance Failure', str(e), EMAIL['user'], EMAIL['password'])

        job.result('error', message=str(e))

        raise e

    print(results)    







