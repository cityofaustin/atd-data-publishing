''' 
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus
'''
import argparse
import os
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.esb.config import cfg
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil


#  invalid XLM characters to be encoded
SPECIAL = {
    "<" : '&lt;',
    ">" : '&gt;',
    "\"" :'&quot;',
    "'" : '&apos;',
    "&" : '&amp;',
}


def encode_special_characters(text):
    #  ESB requires ASCII characters only
    #  We drop non-ASCII characters by encoding as ASCII with "ignore" flag
    text = text.encode("ascii", errors="ignore")
    text = text.decode("ascii")

    #  We also encode invalid XML characters
    for char in SPECIAL.keys():
        text = text.replace(char, SPECIAL[char])

    return text


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
    #  daily subscription limit because we do not
    #  provide reference objects
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


def get_data():
    #  get data at public enpoint and also get
    #  necessary field metadata (which is not public)
    #  field dat ais fetched because we provide a ref_obj array
    return knackpy.Knack(
        ref_obj=cfg['ref_obj'],
        view=cfg['view'],
        scene=cfg['scene'],
        app_id=KNACK_CREDENTIALS[app_name]['app_id'],
        api_key=KNACK_CREDENTIALS[app_name]['api_key']
    )


def build_xml_payload(record):
    record['TMC_ACTIVITY_DETAILS'] = format_activity_details(record)
    record['TMC_ACTIVITY_DETAILS'] = encode_special_characters(record['TMC_ACTIVITY_DETAILS'])
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
    parser = argutil.get_parser(
        'esb_xml_gen.py',
        'Generate XML message to update 311 Service Reqeusts via Enterprise Service Bus.',
        'app_name',
    )
    
    args = parser.parse_args()
    
    return args


def main():

    #  check for data at public endpoint
    data = check_for_data()

    if data:
        #  get data at private enpoint
        kn = get_data()
        
    else:
        logger.info('No new records to process')
        return 0

    #  identify date fields for conversion from mills to unix
    date_fields_kn = [kn.fields[f]['label'] for f in kn.fields if kn.fields[f]['type'] in ['date_time', 'date']]
    kn.data = datautil.mills_to_iso(kn.data, date_fields_kn)

    for record in kn.data:            
        payload = build_xml_payload(record)
        '''
        XML messages are formatted with incremental ATD_ACTIVITY_ID as well as
        database record id. 

        If for some reason this record already has an XML message in queue
        (e.g. the ESB is down), the previous message will be overwritten
        don't change the message format without considering esb_xml_send.py
        '''
        with open('{}/{}_{}.xml'.format(outpath, record['ATD_ACTIVITY_ID'], record['id']), 'w') as fout:
            fout.write(payload)
    
    logger.info('{} records processed.'.format(len(kn.data)))

    return len(kn.data)



if __name__ == '__main__':
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}'
    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    args = cli_args()
    logger.info( 'args: {}'.format( str(args) ))

    app_name = args.app_name
    
    try:
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='knack',
            destination='XML',
            auth=JOB_DB_API_TOKEN)

        #  config
        knack_creds = KNACK_CREDENTIALS
        cfg = cfg['tmc_activities']
        
        #  template output path
        outpath = '{}/{}'.format(ESB_XML_DIRECTORY, 'ready_to_send')
        
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        job.start()

        results = main()

        job.result('success', records_processed=results)

        logger.info('END AT {}'.format( arrow.now() ))

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(str(e))
        logger.error(error_text)
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'ESB XML Generate Failure',
            error_text,
            EMAIL['user'],
            EMAIL['password'])

        job.result('error')

        raise e

