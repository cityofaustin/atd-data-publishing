'''
todo:
- enable email alert
ping field device and update ip comm status in Knack database
command ex: device_status_check.py travel_sensors
'''
import argparse
import json
import logging
from os import system as system_call
import pdb
from platform import system as system_name  
import traceback

import arrow
import knackpy

import _setpath
from config.config import cfg
from config.secrets import *
from util import datautil
from util import emailutil

def ping_ip(ip):
    #  https://stackoverflow.com/questions/2953462/pinging-servers-in-python
    print(ip)
    logging.debug( 'ping {}'.format(ip) )
        
    #  logic for system-specific param
    #  -w / -W is timeout -n/-c is number of packets
    params= "-w 1000 -n 1" if system_name().lower()=="windows" else "-W 1 -c 1"    

    response = system_call("ping " + params + " " + ip)

    logging.debug(str(response))

    if response != 0:
        return "OFFLINE"
    else:
        return "ONLINE"


def main():
    try:
        kn = knackpy.Knack(
            obj=cfg[device_type]['obj'],
            scene=cfg[device_type]['scene'],
            view=cfg[device_type]['view'],
            ref_obj=cfg[device_type]['ref_obj'],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key']
        )

        if out_json:
            out_dir = IP_JSON_DESTINATION
            json_data = datautil.reduce_to_keys(kn.data, out_fields_json)
            filename = '{}/device_data_{}.json'.format(
                out_dir,
                device_type
            )
            
            with open(filename, 'w') as of:
                json.dump(json_data, of)

        for device in kn.data:
            
            if ip_field in device:
                
                if not 'IP_COMM_STATUS' in device:
                    device['IP_COMM_STATUS'] = 'OFFLINE'
                
                state_previous = device['IP_COMM_STATUS']
                
                if device[ip_field]:
                    state_new = ping_ip(device[ip_field])
              
                else:
                    continue

                if state_previous != state_new:

                    device['IP_COMM_STATUS'] = state_new
                    #  all timestamps into and out of knack are naive
                    #  so we create a naive local timestamp by replacing
                    #  a localized timestamp's timezone info with UTC
                    device['COMM_STATUS_DATETIME_UTC'] = arrow.now().replace(tzinfo='UTC').timestamp * 1000
                    device = datautil.reduce_to_keys([device], out_fields_upload)
                    device = datautil.replace_keys(device, kn.field_map)
                    
                    logging.debug(device)
                    
                    response_json = knackpy.update_record(
                            device[0],  #  convert device from array len 1 to dict
                            cfg[device_type]['ref_obj'][0],  #  assumes record object is included in config ref_obj and is the first elem in array
                            'id',
                            knack_creds['app_id'],
                            knack_creds['api_key']
                    )
                    
                    logging.debug(response_json)
                    
        return "done"
    
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
        help='Type of device to ping. \'signal\' or \'travel_sensor\' or \'cctv\'.'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
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