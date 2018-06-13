'''
Get number of device online/offline/no communication and write to log table
in Data Tracker.
'''

import argparse
from collections import defaultdict
import logging
import os
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *

from tdutils import argutil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import datautil
from tdutils import logutil


LOG_OBJ = 'object_131'

def get_log_data():

    return knackpy.Knack(
            obj=LOG_OBJ,
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            rows_per_page=1,
            page_limit=1
        )


def build_payload(data):
    #  create a localized timestamp because Knack assumes timestamps are local
    now = arrow.now().replace(tzinfo='UTC').timestamp * 1000

    date_str = str( arrow.now().date() )
    record_id = '{}-{}'.format(device_type, date_str)

    return {
            'DEVICE_TYPE' :data['DEVICE_TYPE'],
            'ONLINE' : data['ONLINE'],
            'OFFLINE' : data['OFFLINE'],
            'NO_COMMUNICATION' : data['NO COMMUNICATION'],
            'STATUS_DATETIME' : now,
            'RECORD_ID' : record_id
        }


def main(job):

    job.start()

    kn = knackpy.Knack(
        obj=cfg[device_type]['obj'],
        scene=cfg[device_type]['scene'],
        view=cfg[device_type]['view'],
        ref_obj=cfg[device_type]['ref_obj'],
        app_id=knack_creds['app_id'],
        api_key=knack_creds['api_key']
    )
    
    kn_log = get_log_data()

    stats = defaultdict(int)

    stats['DEVICE_TYPE'] = device_type

    for device in kn.data:
        #  count stats only for devices that are TURNED_ON
        if device[status_field] in status_filters:
            status = device['IP_COMM_STATUS']
            stats[status] += 1

    payload = build_payload(stats)
    payload = datautil.replace_keys([payload], kn_log.field_map)

    res = knackpy.record(
        payload[0],
        obj_key=LOG_OBJ,
        app_id= knack_creds['app_id'],
        api_key=knack_creds['api_key'],
        method='create',
    )

    return len(payload)


def cli_args():

    parser = argutil.get_parser(
        'device_status_log.py',
        'Generate connectivity statistics and upload to Knack application.',
        'device_type',
        'app_name'
    )
    
    args = parser.parse_args()
    
    return args


if __name__ == '__main__':
    
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'

    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    args = cli_args()

    device_type = args.device_type
    app_name = args.app_name

    script_id = f'{script_name}_{device_type}'

    primary_key = cfg[device_type]['primary_key']
    status_field = cfg[device_type]['status_field']
    status_filters = cfg[device_type]['status_filter_comm_status']

    knack_creds = KNACK_CREDENTIALS[app_name]

    try:
        job = jobutil.Job(
            name=script_id,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)

        results = main(job)

        job.result('success', records_processed=results)
        logger.info('END AT {}'.format( arrow.now() ))
    
    except Exception as e:
        error_text = traceback.format_exc()
        email_subject = "Device Status Log Failure: {}".format(device_type)
        
        emailutil.send_email(ALERTS_DISTRIBUTION, email_subject, error_text, EMAIL['user'], EMAIL['password'])
        logger.error(error_text)

        job.result('error')
        raise e









