''' 
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus

todo:
- define ESB_LOG_DIRECTORY and update secrets
- email alerts
'''
import argparse
import logging
import os
import pdb

import arrow
import knackpy
import requests

import _setpath
from config.config import cfg_esb
from config.secrets import *
from util import datautil
from util import emailutil

  
def get_record_id_from_file(directory, f):
    record_id = f.split('.')[0]
    return record_id

def get_msg(directory, f):
    #  read xml msg memory
    fin = os.path.join(directory, f)
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


def create_payload(record):
    payload = {
        'id' : record,
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
        help='Name of the knack application that will be accessed'
    )

    args = parser.parse_args()
    
    return(args)


def main(date_time):
    print('starting stuff now')

    try:
        directory = os.fsencode(inpath)

        sent = []
        fail = []
        for file in os.listdir(inpath):
            filename = os.fsdecode(file)
            
            if filename.endswith(".xml"): 
                record_id = get_record_id_from_file(inpath, filename)
                msg = get_msg(inpath, filename)
                res = send_msg(msg, ESB_ENDPOINT['prod'], cfg['path_cert'], cfg['path_key'])
                
                if res.status_code == 200:
                    sent.append(record_id)
                    move_file(inpath, outpath, filename)
                else: 

                    logging.warning( 'Record {} failed to process with error {}'.format(record_id, res.content) ) 
                    fail.append(res.content)

        for record in sent:
            payload = create_payload(record)
            record_id = payload['id']
            
            res = knackpy.update_record(
                payload,
                cfg['obj'],
                'id',
                knack_creds['app_id'],
                knack_creds['api_key']
            )

            if 'total_pages' in res:
                if res['total_pages'] == 0:
                    raise Exception('Failed to update Knack record after ESB pub: {}'.format(record_id) )
            
        #  send email at end if issues
        if fail:
            raise Exception('Records failed to publish to ESB. See log for more details.')

        logging.info('{} records transmitted.'.format(len(sent)))
        

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
    now_s = now.format('YYYY_MM_DD')

    #  init logging 
    logfile = '{}/esb_xml_send_{}.log'.format(LOG_DIRECTORY, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    #  config
    knack_creds = KNACK_CREDENTIALS[app_name]
    cfg = cfg_esb['tmc_activities']
    
    #  template output path
    inpath = '{}/{}'.format(ESB_XML_DIRECTORY, 'ready_to_send')
    outpath = '{}/{}'.format(ESB_XML_DIRECTORY, 'sent')

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))


