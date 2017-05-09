'''
    upload latest b-cycle kiosk data to socrata
'''
import csv
import dropbox
import arrow
import socrata_helpers 
import data_helpers
import agol_helpers
import secrets
import pdb

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



socrata_creds = secrets.SOCRATA_CREDENTIALS
access_token = secrets.DROPBOX_BCYCLE_TOKEN

fieldnames = ['kiosk_id', 'kiosk_name', 'kiosk_status', 'latitude', 'longitude']

#  AGOL CONFIG
SERVICE_URL = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/bcycle_kiosks/FeatureServer/0/'

resource_id = 'qd73-bsdg'
resource_id_pub_log = 'n5kp-f8k4'

path = '/austinbcycletripdata/50StationPlusOld-LongLatInfo.csv'

#  get latest kiosk data from B-cycle dropbox
data = get_dropbox_data(path, access_token)

data = data_helpers.upper_case_keys(data)

data = socrata_helpers.create_location_fields(data)

token = agol_helpers.get_token(secrets.AGOL_CREDENTIALS)

data = data_helpers.replace_keys(data, {"STATUS" : "KIOSK_STATUS"} )

#  replace arcgis online features
agol_payload = agol_helpers.build_payload(data)

del_response = agol_helpers.delete_features(SERVICE_URL, token)

add_response = agol_helpers.add_features(SERVICE_URL, token, agol_payload)

#  reformat for socrata and upsert
data = data_helpers.lower_case_keys(data)

upsert_res = socrata_helpers.upsert_data(socrata_creds, data, resource_id)

#  update publication log
log_entry = socrata_helpers.prep_pub_log(arrow.now(), 'bcycle_kiosk_update', upsert_res)

socrata_helpers.upsert_data(socrata_creds, log_entry, resource_id_pub_log)

print(upsert_res)