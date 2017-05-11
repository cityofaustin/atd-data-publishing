import os
import argparse
import logging
import pdb
import arrow
import knack_helpers
import data_helpers
import secrets
from config import config

'''
ping field device and update ip comm status in Knack database
command ex: device_status_check.py travel_sensors
'''

def get_ips():
    print("getting ip addresses")
    field_dict = knack_helpers.get_fields(knack_objects, knack_creds)
    knack_data = knack_helpers.get_data(knack_scene, knack_view, knack_creds)
    knack_data = knack_helpers.parse_data(knack_data, field_dict, convert_to_unix=True, include_ids=include_ids, id_outfield='KNACK_ID')
    knack_data = data_helpers.reduce_dicts(knack_data, out_fields)
    
    field_lookup = knack_helpers.create_field_lookup(field_dict, parse_raw=True)
    field_lookup['KNACK_ID'] = 'KNACK_ID'

    return (knack_data, field_lookup)


def ping_ip(ip):
    print(ip)
    logging.debug( 'ping {}'.format(ip) )
    response = os.system("ping -n 1 " + ip)
    logging.debug(str(response))

    if response != 0:
        logging.warning( 'no response from {}'.format(ip) )
        return "OFFLINE"

    else:
        return "ONLINE"


def update_record():
    print("update device status and date in knack")


def cli_args():
    parser = argparse.ArgumentParser(prog='device_status+check.py', description='Ping network devices to verify connenectivity.')
    parser.add_argument('device_type', action="store", type=str, help='Type of device to ping. \'travel_sensor\' or \'cctv\'.')
    args = parser.parse_args()
    return(args)


def main():
    
    data = get_ips()

    ip_data = data[0]
    field_lookup = data[1]

    payload = []

    for ip in ip_data:
        state_previous = ip['IP_COMM_STATUS']
        state_new = ping_ip( ip[ip_field] )

        if state_previous != state_new:
            ip['IP_COMM_STATUS'] = state_new
            ip['COMM_STATUS_DATETIME_UTC'] = arrow.now().timestamp
            payload.append(ip)

    if payload:
        payload = data_helpers.unix_to_mills(payload)
        payload = data_helpers.replace_keys(payload, field_lookup, delete_unmatched=True)
        
        for record in payload:
            logging.debug(record)
            response_json = knack_helpers.update_record(record, knack_objects[0], 'KNACK_ID', knack_creds)
            logging.debug(response_json)
    
    else:
        logging.info('No changes to upload')
        return 'No changes to upload'

    return "done"


if __name__ == '__main__':
    
    #  parse command-line arguments
    args = cli_args()
    device_type = args.device_type

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    #  init logging with one logfile per dataset per day
    logfile = './log/device_status_check_{}_{}.log'.format(device_type, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info( 'args: {}'.format( str(args) ) )
    logging.info('START AT {}'.format(str(now)))
        
    knack_view = config[device_type]['view'] 
    knack_scene = config[device_type]['scene']
    knack_objects = config[device_type]['objects']
    ip_field = config[device_type]['ip_field']
    include_ids = config[device_type]['include_ids']
    knack_creds = secrets.KNACK_CREDENTIALS
    out_fields = ['KNACK_ID', ip_field, 'IP_COMM_STATUS', 'COMM_STATUS_DATETIME_UTC']

    results = main()
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)