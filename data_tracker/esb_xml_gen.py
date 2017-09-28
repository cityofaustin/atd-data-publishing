    ''' 
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus

todo:
- email alerts
'''
import argparse
import logging
import pdb

import arrow
import knackpy

import _setpath
from config.config import cfg_esb
from config.secrets import *
from util import datautil
from util import emailutil


def get_csr_filters(emi_field, esb_status_field, esb_status_match):
    #  construct a knack filter object
    filters = {
            'match': 'and',
            'rules': [
                {
                   'field': emi_field,
                   'operator': 'is not blank'
                },
                {
                    'field': esb_status_field,
                    'operator': 'is',
                    'value': esb_status_match
                }
            ]
        };

    return filters


def check_for_data():
    #  check for data at public endpoint
    #  this api call does not count against
    #  daily subscription limit
    kn = knackpy.Knack(
        view=cfg['view'],
        scene=cfg['scene'],
        app_id=KNACK_CREDENTIALS[app_name]['app_id'],
        api_key='knack',
        page_limit=1,
        rows_per_page=1
    )
    
    if kn.data_raw:
        return True
    else:
        return False


def get_data(filters):
    return knackpy.Knack(
        obj=cfg['obj'],
        app_id=KNACK_CREDENTIALS[app_name]['app_id'],
        api_key=KNACK_CREDENTIALS[app_name]['api_key'],
        filters=filters
    )


def build_xml_payload(record):
    record['TMC_ACTIVITY_DETAILS'] = format_activity_details(record)
    record['PUBLICATION_DATETIME'] = arrow.now().format()

    with open(cfg['template'], 'r') as fin:
        template = fin.read()
        return template.format(**record)


def format_activity_details(record):
    activity = record['TMC_ACTIVITY']
    details = record['TMC_ACTIVITY_DETAILS']

    if activity and details:
        return '{} - {}'.format(activity, details)
    elif activity or details:
        return '{}{}'.format(activity, details)
    else:
        return ''


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
        #  check for data at public endpoint
        if check_for_data():
            #  get data at private enpoint
            filters = get_csr_filters(cfg['emi_field'], cfg['esb_status_field'], cfg['esb_status_match'])
            kn = get_data(filters)

        else:
            logging.info('No new records to process')
            return None

        #  identify date fields for conversion from mills to unix
        date_fields_kn = [kn.fields[f]['label'] for f in kn.fields if kn.fields[f]['type'] in ['date_time', 'date']]
        kn.data = datautil.mills_to_iso(kn.data, date_fields_kn)

        for record in kn.data:
            payload = build_xml_payload(record)
            #  ESB requires ASCII characters only
            #  We drop non-ASCII characters by encoding as ASCII with "ignore" flag
            payload = payload.encode("ascii", errors="ignore")
            payload = payload.decode("ascii")
            
            with open('{}/{}_{}.xml'.format(outpath, record['id'], arrow.now().timestamp), 'w') as fout:
                fout.write(payload)
            
        return 'GOOD JOB!'

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        
        # emailutil.send_email(
        #     ALERTS_DISTRIBUTION,
        #     'Location Update Failure',
        #     str(e),
        #     EMAIL['user'],
        #     EMAIL['password']
        # )

        raise e

if __name__ == '__main__':
    args = cli_args()
    app_name = args.app_name

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    #  init logging 
    logfile = '{}/esb_xml_gen_{}.log'.format(LOG_DIRECTORY, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    #  config
    knack_creds = KNACK_CREDENTIALS
    cfg = cfg_esb['tmc_activities']
    
    #  template output path
    outpath = '{}/{}'.format(ESB_XML_DIRECTORY, 'ready_to_send')

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))


