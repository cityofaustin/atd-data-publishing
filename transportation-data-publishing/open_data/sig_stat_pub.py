import os
import pdb
import sys

import arrow

import _setpath
from config.knack.config import cfg
from config.secrets import *
from tdutils import kitsutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil
from tdutils import socratautil

# test data
# KITS_DATA = [{'KITS_ID': 50, 'OPERATION_STATE_DATETIME': datetime.datetime(2018, 5, 16, 13, 45, 29, 17000), 'OPERATION_STATE': 2, 'PLAN_ID':0, 'SIGNAL_ID': 617}, {'KITS_ID': 50, 'OPERATION_STATE_DATETIME': datetime.datetime(2018, 5, 16, 13, 45, 29, 17000), 'OPERATION_STATE': 1, 'PLAN_ID':0, 'SIGNAL_ID': 110}]

def add_ids(records, primary_key='signal_id', id_field='record_id'):
    '''
    Generate a unique record ID which is a concatenation of the signal ID and the current time
    '''
    now = arrow.now().timestamp

    for record in records:
        if not record.get('record_id'):
            record['record_id'] = '{}_{}'.format(record[primary_key], now)

    return records


def add_timestamps(records, timestamp_field='processed_datetime'):
    now = arrow.now().timestamp

    for record in records:
        record[timestamp_field] = now

    return records



def main():
    # get current traffic signal data from Socrata      
    socr = socratautil.Soda(resource=SOCR_SIG_RES_ID, fetch_metadata=True)
    signal_data = socr.data

    kits_query = kitsutil.status_query()

    kits_data = kitsutil.data_as_dict(
        KITS_CREDENTIALS,
        kits_query
    )

    kits_data = datautil.replace_timezone(kits_data, ['OPERATION_STATE_DATETIME'])

    kits_data = datautil.stringify_key_values(kits_data)
    
    #  verify the KITS data is current
    #  sometimes the signal status service goes down
    #  in which case contact ATMS support
    stale = kitsutil.check_for_stale(kits_data, 'OPERATION_STATE_DATETIME')

    #  filter KITS data for statuses of concern
    kits_data = datautil.filter_by_val(
        kits_data,
        'OPERATION_STATE',
        FLASH_STATUSES
    )

    #  append kits data to signal data
    if kits_data:
        new_data = datautil.lower_case_keys(kits_data)

        new_data = datautil.merge_dicts(
            signal_data,
            new_data,
            'signal_id',
            ['operation_state_datetime', 'operation_state', 'plan_id']
        )

        new_data = datautil.stringify_key_values(new_data)

    else:
        new_data = []
    
    #  get current signal status DATASET and metadata from socrata
    sig_status = socratautil.Soda(resource=SOCR_SIG_STAT_RES_ID, fetch_metadata=True)

    #  add special socrata deleted field
    #  required for sending delete requests to socrata
    fieldnames = sig_status.fieldnames + [':deleted']

    #  transform signal status socrata data for comparison 
    #  with "new" data from kits
    sig_status_data = datautil.reduce_to_keys(sig_status.data, fieldnames)
    date_fields = sig_status.date_fields
    sig_status_data = socratautil.strip_geocoding(sig_status_data)
    sig_status_data = datautil.stringify_key_values(sig_status_data)

    #  identify signals whose status (OPERATION_STATE) has changed
    cd_results = datautil.detect_changes(
        sig_status_data,
        new_data,
        'signal_id',
        #  only a change in operation state
        #  triggers an update to socrata DATASET
        keys=['operation_state']  
    )

    for change_type in cd_results.keys():
        #  log signals whose status has changed
        if len(cd_results[change_type]) > 0:
            logger.info(
                '{}: {}'.format(change_type, len(cd_results[change_type]))
            )

    
    if cd_results['new'] or cd_results['change'] or cd_results['delete']:
                
        adds = add_ids(cd_results['new'])

        deletes = socratautil.prepare_deletes(cd_results['delete'], 'signal_id')

        payload = adds + cd_results['change']

        payload = add_timestamps(payload)

        payload = payload + deletes

        payload = datautil.reduce_to_keys(
            payload,
            fieldnames
        )

        results = socratautil.Soda(
            auth=SOCRATA_CREDENTIALS,
            records=payload,
            resource=SOCR_SIG_STAT_RES_ID,
            date_fields=None,
            lat_field='location_latitude',
            lon_field='location_longitude',
            location_field='location',
            replace=False)

        return len(payload)

    else:
        return 0

    

if __name__=='__main__': 

    SOCR_SIG_RES_ID = 'xwqn-2f78'
    SOCR_SIG_STAT_RES_ID = '5zpr-dehc'
    FLASH_STATUSES = ['1', '2', '3']

    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    
    logger = logutil.timed_rotating_log(logfile)

    try:
        logger.info('START AT {}'.format( arrow.now() ))

        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='kits',
            destination='socrata',
            auth=JOB_DB_API_TOKEN)

        job.start()

        results = main()
        
        logger.info('END AT {}'.format( arrow.now() ))

        job.result('success', records_processed=results)

    except Exception as e:
        logger.info(e)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: Signal Status Update Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        job.result('error', message=str(e))

        raise e






