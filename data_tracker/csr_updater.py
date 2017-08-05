#  update CSR records with status informat from Data Tracker (Knack)
#  todo:
#  validation:  ensure only mapped exist values in message
import argparse
import logging
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import datautil
from util import emailutil


def build_xml_payload(record):
    record['LAST_ACTIVITY_DETAILS'] = format_activity_details(record)
    record['PUBLICATION_DATETIME'] = arrow.now().format()

    with open(template_path, 'r') as fin:
        template = fin.read()
        return template.format(**record)


def format_activity_details(record):
    activity = record['LAST_ACTIVITY']
    details = record['LAST_ACTIVITY_DETAILS']

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
        kn = knackpy.Knack(
            view=view,
            scene=scene,
            ref_obj=[obj],
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key']
        )

        if not kn.data:
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
            
            with open('{}/{}.xml'.format(outpath, record['id']), 'w') as fout:
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
    logfile = '{}/csr_updater_{}.log'.format(LOG_DIRECTORY, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    #  config
    knack_creds = KNACK_CREDENTIALS
    obj = 'object_83'
    view = 'view_1445'
    scene = 'scene_514'
    template_path = '../config/esb_csr_template.xml'
    
    #  template output path
    outpath = '{}/xml'.format(LOG_DIRECTORY)

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))




