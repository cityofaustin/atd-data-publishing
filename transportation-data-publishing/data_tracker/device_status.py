'''
Ping network devices and update ip comm status in Knack database.

command ex: device_status_check.py travel_sensors data_tracker_prod

#TODO
- move write to JSON to a separate script. it doesn't belong here.
'''
import argparse
import json
from multiprocessing.dummy import Pool as ThreadPool 
import os
from os import system as system_call
import pdb
from platform import system as system_name  
import socket
import traceback

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil


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

    logger.debug(str(response))

    if response != 0:
        return 'OFFLINE'
    else:
        return 'ONLINE'


def open_socket(ip, port, timeout=3):
    with socket.socket() as s:
        print(ip)
        try:
            s.settimeout(timeout)
            s.connect((ip, port))
            return 'ONLINE'
        except OSError:
            return 'OFFLINE'
            

def get_status(device):
    
    #  get old IP status, setting it to NO COMMUNICATION if not present    
    state_previous = device.setdefault('IP_COMM_STATUS', 'NO COMMUNICATION')
    
    ip = device.get(ip_field)

    if ip:
        if args.device_type != 'gridsmart':
            state_new = ping_ip(device[ip_field])
        
        else:
            '''
            Gridsmart default port is 8902
            '''
            state_new = open_socket(device[ip_field], port=8902)
    
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

                res = knackpy.record(
                    result[0],
                    obj_key=cfg[device_type]['ref_obj'][0],  #  assumes record object is included in config ref_obj and is the first elem in array,
                    app_id= knack_creds['app_id'],
                    api_key=knack_creds['api_key'],
                    method='update',
                )

        # close the pool and wait for the work to finish 
        pool.close() 
        pool.join() 
        
        return True
    


def cli_args():

    parser = argutil.get_parser(
        'device_status_check.py',
        'Ping network devices to verify connenectivity.',
        'device_type',
        'app_name',
        '--json',
        '--replace'
    )
    
    args = parser.parse_args()
    
    return args


if __name__ == '__main__':
    
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    
    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    #  parse command-line arguments
    args = cli_args()
    logger.info( 'args: {}'.format( args ))

    device_type = args.device_type
    out_json = args.json
    app_name = args.app_name
    primary_key = cfg[device_type]['primary_key']
    ip_field = cfg[device_type]['ip_field']

    script_id = '{}_{}'.format(
        script_name,
        args.device_type)

    try:
        job = jobutil.Job(
            name=script_id,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)
     
        job.start()

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

        if (results):
            job.result('success')

        logger.info('END AT {}'.format( arrow.now() ))

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(error_text)

        email_subject = "Device Status Check Failure: {}".format(device_type)
        emailutil.send_email(ALERTS_DISTRIBUTION, email_subject, error_text, EMAIL['user'], EMAIL['password'])
        
        job.result('error', message=str(e))

        raise e


print(results)