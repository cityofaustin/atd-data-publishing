'''
Get number of device online/offline/no communication and write to log table in Knack.
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
from util import emailutil
from util import datautil
from util import logutil


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


def main():
    try:
        #  get device data from Knack application
        kn = knackpy.Knack(
            obj=cfg[device_type]['obj'],
            scene=cfg[device_type]['scene'],
            view=cfg[device_type]['view'],
            ref_obj=cfg[device_type]['ref_obj'],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key']
        )
        
        #  get log file metadata
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

        return True
    
    except Exception as e:
        print('Failed to process data for {}'.format(str(now)) )
        error_text = traceback.format_exc()
        email_subject = "Device Status Log Failure: {}".format(device_type)
        emailutil.send_email(ALERTS_DISTRIBUTION, email_subject, error_text, EMAIL['user'], EMAIL['password'])
        logger.error(error_text)
        print(e)
        raise e


def cli_args():
    parser = argparse.ArgumentParser(
        prog='device_status_log.py',
        description='Generate connectivity statistics and upload to Knack application.'
    )

    parser.add_argument(
        'device_type',
        action="store",
        type=str,
        choices=['signals', 'travel_sensors', 'cameras', 'gridsmart'],
        help='Type of device to calculate.'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        choices=['data_tracker_prod', 'data_tracker_test'],
        help='Name of the knack application that will be accessed. e.g. \'data_tracker_prod\''
    )

    args = parser.parse_args()
    return(args)


if __name__ == '__main__':
    
    script = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script}.log'
    logger = logutil.timed_rotating_log(logfile)
    
    now = arrow.now()
    logger.info('START AT {}'.format(str(now)))

    #  parse command-line arguments
    args = cli_args()
    logger.info( 'args: {}'.format( str(args) ))

    device_type = args.device_type
    app_name = args.app_name
    
    primary_key = cfg[device_type]['primary_key']
    status_field = cfg[device_type]['status_field']
    status_filters = cfg[device_type]['status_filter_comm_status']

    knack_creds = KNACK_CREDENTIALS[app_name]

    results = main()
    logger.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)