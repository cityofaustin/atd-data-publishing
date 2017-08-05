'''
    upload latest b-cycle kiosk data to socrata
'''
import csv
import pdb

import arrow
import dropbox

import _setpath
from config.secrets import *
from util import agolutil
from util import datautil
from util import emailutil
from util import socratautil 


def get_dropbox_data(path, token):
    '''
    get dropbox csv and return as list of dicts
    '''
    print("get dropbox data")
    client = dropbox.client.DropboxClient(token)
    f, metadata = client.get_file_and_metadata(path)
    
    content = f.read()
    
    data_string = content.decode('utf-8')
    data = data_string.splitlines()
    #  del(data[0])  # remove header row

    reader = csv.DictReader(data)

    return list(reader)


try:
    socrata_creds = SOCRATA_CREDENTIALS
    access_token = DROPBOX_BCYCLE_TOKEN

    fieldnames = ['kiosk_id', 'kiosk_name', 'kiosk_status', 'latitude', 'longitude']

    #  AGOL CONFIG
    SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/bcycle_kiosks/FeatureServer/0/'

    resource_id = 'qd73-bsdg'
    resource_id_pub_log = 'n5kp-f8k4'

    path = '/austinbcycletripdata/50StationPlusOld-LongLatInfo.csv'

    #  get latest kiosk data from B-cycle dropbox
    data = get_dropbox_data(path, access_token)
    data = datautil.upper_case_keys(data)
    data = socratautil.create_location_fields(data)

    token = agolutil.get_token(AGOL_CREDENTIALS)
    data = datautil.replace_keys(data, {"STATUS" : "KIOSK_STATUS"} )
    data = datautil.filter_by_key_exists(data, 'LATITUDE')

    #  replace arcgis online features
    agol_payload = agolutil.build_payload(data)
    del_response = agolutil.delete_features(SERVICE_URL, token)
    add_response = agolutil.add_features(SERVICE_URL, token, agol_payload)

    #  reformat for socrata and upsert
    data = datautil.lower_case_keys(data)

    upsert_res = socratautil.upsert_data(socrata_creds, data, resource_id)

    #  update publication log
    log_entry = socratautil.prep_pub_log(arrow.now(), 'bcycle_kiosk_update', upsert_res)

    socratautil.upsert_data(socrata_creds, log_entry, resource_id_pub_log)

    print(upsert_res)

except Exception as e:
    print('Failed to process bcycle kiosk data for {}'.format(arrow.now().format()))
    print(e)
    emailutil.send_email(ALERTS_DISTRIBUTION, 'BCycle Kiosk Update Failure', str(e), EMAIL['user'], EMAIL['password'])
    raise e