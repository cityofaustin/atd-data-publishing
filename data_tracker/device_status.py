'''
Ping network devices and update ip comm status in Knack database.

command ex: device_status_check.py travel_sensors data_tracker_prod
'''
import argparse
import json
import logging
from os import system as system_call
import pdb
from multiprocessing.dummy import Pool as ThreadPool 
from platform import system as system_name  
import traceback

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *
from util import datautil
from util import emailutil


def ping_ip(ip, timeout=3):
    '''
    Ping an IP address
    https://stackoverflow.com/questions/2953462/pinging-servers-in-python
    '''
    if system_name().lower() == "windows":
        #  -w is timeout -n is number of packets
        params = "-w {} -n 1".format(timeout * 1000) # convert seconds to mills for non-windows

    else:
        #  -W is timeout -c is number of packets
        params = "-W {} -c 1".format(timeout)   

    response = system_call("ping " + params + " " + ip)

    logging.debug(str(response))

    if response != 0:
        return "OFFLINE"
    else:
        return "ONLINE"


def get_status(device):
    
    #  get old IP status, setting it to NO COMMUNICATION if not present    
    state_previous = device.setdefault('IP_COMM_STATUS', 'NO COMMUNICATION')
    
    ip = device.get(ip_field)

    if ip:
        state_new = ping_ip(device[ip_field])
    
    else:
        #  set to NO COMMUINICATION if no IP address
        state_new='NO COMMUNICATION'

    if state_previous != state_new:

        device['IP_COMM_STATUS'] = state_new
        #  timestamps into and out of knack are naive
        #  so we create a naive local timestamp by replacing
        #  a localized timestamp's timezone info with UTC
        device['COMM_STATUS_DATETIME_UTC'] = arrow.now().replace(tzinfo='UTC').timestamp * 1000

        return device

    else:
        return None



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

        #  optionally write to JSON
        #  this is a special case for CCTV cameras.
        #  we copy the JSON to our internal web server
        if out_json:
            out_dir = IP_JSON_DESTINATION
            json_data = datautil.reduce_to_keys(kn.data, out_fields_json)
            filename = '{}/device_data_{}.json'.format(
                out_dir,
                device_type
            )
            
            with open(filename, 'w') as of:
                json.dump(json_data, of)

        pool = ThreadPool(8)

        results = pool.map(get_status, kn.data)
        
        for result in results:
            '''
            Result is None if status has not changed. Otherwise result
            is device record dict
            '''
            if result:
                #  format for upload to Knack
                result = datautil.reduce_to_keys([result], out_fields_upload)
                result = datautil.replace_keys(result, kn.field_map)

                response_json = knackpy.update_record(
                    result[0],
                    cfg[device_type]['ref_obj'][0],  #  assumes record object is included in config ref_obj and is the first elem in array
                    knack_creds['app_id'],
                    knack_creds['api_key']
                )

        # close the pool and wait for the work to finish 
        pool.close() 
        pool.join() 
        
        return True
    
    except Exception as e:
        print('Failed to process data for {}'.format(str(now)) )
        error_text = traceback.format_exc()
        email_subject = "Device Status Check Failure: {}".format(device_type)
        emailutil.send_email(ALERTS_DISTRIBUTION, email_subject, error_text, EMAIL['user'], EMAIL['password'])
        logging.error(error_text)
        print(e)
        raise e


def cli_args():
    parser = argparse.ArgumentParser(
        prog='device_status_check.py',
        description='Ping network devices to verify connenectivity.'
    )

    parser.add_argument(
        'device_type',
        action="store",
        type=str,
        choices=['signals', 'travel_sensors', 'cameras'],
        help='Type of device to ping.'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        choices=['data_tracker_prod', 'data_tracker_test'],
        help='Name of the knack application that will be accessed. e.g. \'data_tracker_prod\''
    )

    parser.add_argument(
        '-json',
        action='store_true',
        default=False,
        help='Write device data to JSON.'
    )

    args = parser.parse_args()
    return(args)


if __name__ == '__main__':
    #  parse command-line arguments
    args = cli_args()
    device_type = args.device_type
    out_json = args.json
    app_name = args.app_name

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    #  init logging with one logfile per dataset per day
    
    logfile = '{}/device_status_check_{}_{}.log'.format(
        LOG_DIRECTORY,
        device_type,
        now_s
    )
    
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO
    )

    logging.info( 'args: {}'.format( str(args) ) )
    logging.info('START AT {}'.format(str(now)))
        
    primary_key = cfg[device_type]['primary_key']
    ip_field = cfg[device_type]['ip_field']

    knack_creds = KNACK_CREDENTIALS[app_name]
    
    out_fields_upload = [
        'id',
        ip_field,
        'IP_COMM_STATUS',
        'COMM_STATUS_DATETIME_UTC'
    ]

    out_fields_json = [
        'id',
        ip_field,
        'IP_COMM_STATUS',
        'COMM_STATUS_DATETIME_UTC',
        primary_key
    ]

    results = main()
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)