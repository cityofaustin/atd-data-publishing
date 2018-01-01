'''
Get latest b-cycle kiosk data from Dropbox and upload to Socrata
'''
import csv
import pdb

import arrow
import dropbox

import _setpath
from config.knack.config import cfg
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
    #  config
    start_time = arrow.now().timestamp
    fieldnames = ['kiosk_id', 'kiosk_name', 'kiosk_status', 'latitude', 'longitude']
    service_url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/bcycle_kiosks/FeatureServer/0/'
    resource_id = 'qd73-bsdg'
    pub_log_id = cfg['publication_log']['socrata_resource_id']
    script_name = __file__.split('.')[0]
    dataset='B-Cycle Kiosk Locations'
    path = '/austinbcycletripdata/50StationPlusOld-LongLatInfo.csv'

    #  get latest kiosk data from B-cycle dropbox
    data = get_dropbox_data(path, DROPBOX_BCYCLE_TOKEN)
    data = datautil.upper_case_keys(data)
    data = socratautil.create_location_fields(data)

    token = agolutil.get_token(AGOL_CREDENTIALS)
    data = datautil.replace_keys(data, {"STATUS" : "KIOSK_STATUS"} )
    data = datautil.filter_by_key_exists(data, 'LATITUDE')

    #  replace arcgis online features
    agol_payload = agolutil.build_payload(data)
    del_response = agolutil.delete_features(service_url, token)
    add_response = agolutil.add_features(service_url, token, agol_payload)

    #  reformat for socrata and upsert
    data = datautil.lower_case_keys(data)
    upsert_res = socratautil.upsert_data(SOCRATA_CREDENTIALS, data, resource_id)

    #  update publication log
    log_payload = socratautil.pub_log(
        name=script_name,
        start=start_time,
        end=arrow.now().timestamp,
        resource=resource_id,
        dataset=dataset
    )

    log_payload = socratautil.handle_response(upsert_response, log_payload)

    socratautil.upsert_data(SOCRATA_CREDENTIALS, log_payload, resource_id_pub_log)


except Exception as e:
    print('Failed to process bcycle kiosk data for {}'.format(arrow.now().format()))
    print(e)
    emailutil.send_email(ALERTS_DISTRIBUTION, 'BCycle Kiosk Update Failure', str(e), EMAIL['user'], EMAIL['password'])
    raise e