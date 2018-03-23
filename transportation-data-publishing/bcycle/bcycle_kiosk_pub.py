'''
Get latest b-cycle kiosk data from Dropbox and upload to Socrata
'''
import csv
import os
import pdb

import arrow
import dropbox
import requests

import _setpath
from config.secrets import *
from util import agolutil
from util import datautil
from util import emailutil
from util import logutil
from util import socratautil


def get_data(path, token):
    '''
    Get trip data file as string from dropbox
    '''
    logger.info(f'Get data for {path}')

    dbx = dropbox.Dropbox(token)

    metadata, res = dbx.files_download(path)
    res.raise_for_status()
    
    return res.text


def handle_data(data):
    '''
    Convert data file string to csv dict.
    '''
    rows = data.splitlines()
    reader = csv.DictReader(rows)
    return list(reader)


if __name__ == '__main__':
    script = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script}.log'
    logger = logutil.timed_rotating_log(logfile)

    try:
        #  config
        logger.info('START AT {}'.format( arrow.now()) )

        agol_service_url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/bcycle_kiosks/FeatureServer/0/'
        socrata_resource_id = 'qd73-bsdg'
        dropbox_path = '/austinbcycletripdata/50StationPlusOld-LongLatInfo.csv'

        #  get latest kiosk data from B-cycle dropbox
        data = get_data(dropbox_path, DROPBOX_BCYCLE_TOKEN)
        data = handle_data(data)
        data = datautil.upper_case_keys(data)
        data = socratautil.create_location_fields(data)

        token = agolutil.get_token(AGOL_CREDENTIALS)
        data = datautil.replace_keys(data, {'STATUS' : 'KIOSK_STATUS'} )
        data = datautil.filter_by_key_exists(data, 'LATITUDE')
        
        #  replace arcgis online features
        agol_payload = agolutil.build_payload(data)
        del_response = agolutil.delete_features(agol_service_url, token)
        add_response = agolutil.add_features(agol_service_url, token, agol_payload)
        
        #  reformat for socrata and upsert
        data = datautil.lower_case_keys(data)
        upsert_res = socratautil.upsert_data(SOCRATA_CREDENTIALS, data, socrata_resource_id)


    except Exception as e:
        logger.error(e)
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: B-Cycle Kiosk Pub',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e



