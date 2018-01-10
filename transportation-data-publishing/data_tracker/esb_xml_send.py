''' 
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus
'''
import argparse
import logging
import os
import pdb

import arrow
import knackpy
import requests

import _setpath
from config.esb.config import cfg
from config.secrets import *
from util import datautil
from util import emailutil


def get_record_id_from_file(directory, file):
    '''
    Extract Knack record id from filename.

    Expects XML messages to be namedw with incremental record ID as well as
    Knack database ID. The former is used to sort records in chronological
    order (not returned by this function) and the latter is used to update
    the Knack record with a 'SENT' status when message has been successfully
    transmitted to ESB.

    Expected format is incrementaId_knackId.xml. E.g. 10034_axc3345f23msf0.xml
    '''
    record_data = file.split('.')[0]
    return record_data.split('_')[1]


def get_sorted_file_list(path):
    '''
    Retrieve XML files from directory and return a sorted list of
    files based on filename.

    Assumes ascendant sorting of filenames is equivalent to sorting
    oldest to newest records in database. This is accomplished by naming files
    with their incremental record ID via esb_xml_gen.py

    Returns array of filenames sorted A-Z, aka oldest to newest.
    '''
    files = []

    for file in os.listdir(path):
        filename = os.fsdecode(file)
            
        if filename.endswith(".xml"): 
            files.append(file)
    
    files.sort()

    return files


def get_msg(directory, file):
    #  read xml msg memory
    fin = os.path.join(directory, file)
    with open(fin, 'r') as msg:
        return msg.read()

def send_msg(msg, endpoint, path_cert, path_key, timeout=20):
    headers = {'content-type': 'text/xml'}
    res = requests.post(
        endpoint,
        data=msg,
        headers=headers,
        timeout=timeout,
        verify=False,
        cert=(path_cert, path_key)
    )

    return res

def move_file(old_dir, new_dir, f):
    infile = os.path.join(old_dir, f)
    outfile = os.path.join(new_dir, f)
    os.rename(infile, outfile)


def create_payload(record_id):
    payload = {
        'id' : record_id,
        cfg['esb_status_field'] : 'SENT'
    }
    return payload


def cli_args():
    parser = argparse.ArgumentParser(
        prog='csr updater',
        description='Update service requests in the CSR system from Data Tracker'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        choices=['data_tracker_prod', 'data_tracker_test'],
        help='Name of the knack application that will be accessed'
    )

    args = parser.parse_args()
    
    return(args)


def main(date_time):
    print('starting stuff now')

    try:
        directory = os.fsencode(inpath)
        
        '''
        Get files in order by incremental ID. This ensures messages
        are transmitted chronologically.
        '''
        files = get_sorted_file_list(inpath)
        
        for filename in files:
            '''
            Extract record id, send message to ESB, move file to 'sent' folder,
            and update Knack record with status of SENT.
            '''
            record_id = get_record_id_from_file(inpath, filename)
            
            msg = get_msg(inpath, filename)
            
            res = send_msg(msg, ESB_ENDPOINT['prod'], cfg['path_cert'], cfg['path_key'])

            res.raise_for_status()
            
            move_file(inpath, outpath, filename)
                        
            ''' Update Knack Record '''
            payload = create_payload(record_id)
            
            res = knackpy.update_record(
                payload,
                cfg['obj'],
                knack_creds['app_id'],
                knack_creds['api_key']
            )

        logging.info('{} records transmitted.'.format(len(files)))
        

    except Exception as e:
        print('Failed to publish ESB msg data for {}'.format(date_time))
        print(e)
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'ESB Publication Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e

if __name__ == '__main__':
    args = cli_args()
    app_name = args.app_name

    now = arrow.now()

    #  init logging 
    script = os.path.basename(__file__).replace('.py', '.log')
    logfile = f'{LOG_DIRECTORY}/{script}'
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    #  config
    knack_creds = KNACK_CREDENTIALS[app_name]
    cfg = cfg['tmc_activities']
    
    #  template output path
    inpath = '{}/{}'.format(ESB_XML_DIRECTORY, 'ready_to_send')
    outpath = '{}/{}'.format(ESB_XML_DIRECTORY, 'sent')

    if not os.path.exists(inpath):
        os.makedirs(inpath)

    if not os.path.exists(outpath):
        os.makedirs(outpath)

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))


