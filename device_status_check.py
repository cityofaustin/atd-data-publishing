import os
import argparse
import logging
import json
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
    return (knack_data, field_dict)


def parseIps(data, field_dict, outfields):
    parsed_data = knack_helpers.parse_data(data, field_dict, convert_to_unix=True, include_ids=include_ids, id_outfield='KNACK_ID')
    parsed_data = data_helpers.reduce_dicts(parsed_data, outfields)
    field_lookup = knack_helpers.create_field_lookup(field_dict, parse_raw=True)
    field_lookup['KNACK_ID'] = 'KNACK_ID'
    return (parsed_data, field_lookup)


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


def main():
    
    knack_data = get_ips()
    records = knack_data[0]
    field_dict = knack_data[1]

    if out_json:
        parsed_data = parseIps(records, field_dict, out_fields_json)
        pdb.set_trace()
        json_data = parsed_data[0]
        field_lookup = parsed_data[1]

        out_dir = secrets.IP_JSON_DESTINATION
        filename = 'log/data.json'
        with open(filename, 'w') as of:
            json.dump(json_data, of)

    parsed_data = parseIps(records,field_dict, out_fields_upload)
    
    for ip in ip_data:

        if ip_field in ip:
            state_previous = ip['IP_COMM_STATUS']
            state_new = ping_ip( ip[ip_field] )

            if state_previous != state_new:
                record = []

                ip['IP_COMM_STATUS'] = state_new
                ip['COMM_STATUS_DATETIME_UTC'] = arrow.now().timestamp
                
                record = [ip]
                record = data_helpers.unix_to_mills(record)
                record = data_helpers.replace_keys(record, field_lookup, delete_unmatched=True)
                logging.debug(record)
                response_json = knack_helpers.update_record(record[0], knack_objects[0], 'KNACK_ID', knack_creds)
                logging.debug(response_json)
  
    return "done"


def cli_args():
    parser = argparse.ArgumentParser(prog='device_status+check.py', description='Ping network devices to verify connenectivity.')
    parser.add_argument('device_type', action="store", type=str, help='Type of device to ping. \'travel_sensor\' or \'cctv\'.')
    parser.add_argument('-json', action='store_true', default=False, help='Write device data to JSON.')
    args = parser.parse_args()
    return(args)


if __name__ == '__main__':
    
    #  parse command-line arguments
    args = cli_args()
    device_type = args.device_type
    out_json = args.json
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
    primary_key = config[device_type]['primary_key']
    knack_creds = secrets.KNACK_CREDENTIALS
    out_fields_upload = ['KNACK_ID', ip_field, 'IP_COMM_STATUS', 'COMM_STATUS_DATETIME_UTC']
    out_fields_json = ['KNACK_ID', ip_field, 'IP_COMM_STATUS', 'COMM_STATUS_DATETIME_UTC', primary_key]

    results = main()
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)








