'''
    compare socrata and dropbox b-cycle data
    upload latest data to socrata as needed
'''
import csv
import sys

import arrow
import dropbox
import requests

import _setpath
from config.secrets import *
from util import emailutil
from util import socratautil


def max_date_socrata(resource_id):
    url = 'https://data.austintexas.gov/resource/{}.json?$query=SELECT max(checkout_date) as date'.format(resource_id)

    try:
        res = requests.get(url, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()[0]['date']


def data_exists_dropbox(path, token):
    '''
        Check if last month's trip data exists on dropbox
    '''
    client = dropbox.client.DropboxClient(token)

    try:
        client.metadata(path)
        return True
    
    except:
        return False


def get_dropbox_data(path, token):
    '''
    get dropbox csv and return as list of dicts
    '''
    client = dropbox.client.DropboxClient(token)
    f, metadata = client.get_file_and_metadata(path)
    
    content = f.read()
    
    data_string = content.decode('utf-8')
    data = data_string.splitlines()
    del(data[0])  # remove header row

    reader = csv.DictReader(data, fieldnames=fieldnames)

    return list(reader)

try:
    socrata_creds = SOCRATA_CREDENTIALS
    access_token = DROPBOX_BCYCLE_TOKEN

    fieldnames = ('trip_id', 'membership_type', 'bicycle_id', 'checkout_date', 'checkout_time', 'checkout_kiosk_id', 'checkout_kiosk', 'return_kiosk_id', 'return_kiosk', 'trip_duration_minutes')

    one_month_ago = arrow.now().replace(months=-1)
    dropbox_year = one_month_ago.format('YYYY')
    dropbox_month = one_month_ago.format('MM')

    resource_id_query = 'cwi3-ckqi'
    resource_id_publish = 'tyfh-5r8s'
    resource_id_pub_log = 'n5kp-f8k4'

    socrata_dt = max_date_socrata(resource_id_query)
    socrata_month = arrow.get(socrata_dt).format('MM')


    if dropbox_month == socrata_month:
        #  data is already up to date on socrata
        print("trip data already is up to date on socrata.")
        
        #  update publication log
        upsert_res = { 'Errors' : 0, 'message' : 'No new trip data detected' , 'Rows Updated' : 0, 'Rows Created' : 0, 'Rows Deleted' : 0 }
        log_entry = socratautil.prep_pub_log(arrow.now(), 'bcycle_trip_update', upsert_res)
        socratautil.upsert_data(socrata_creds, log_entry, resource_id_pub_log)

        sys.exit()

    else:
        current_file = 'TripReport-{}{}.csv'.format(dropbox_month, dropbox_year)
        root = 'austinbcycletripdata'  #  note the lowercase-ness 
        path = '/{}/{}/{}'.format(root, dropbox_year, current_file)

        if data_exists_dropbox(path, access_token):
            print("getting new data")
            data = get_dropbox_data(path, access_token)

            print("upserting new data to socrata")
            upsert_res = socratautil.upsert_data(socrata_creds, data, resource_id_publish)

            #  update publication log
            log_entry = socratautil.prep_pub_log(arrow.now(), 'bcycle_trip_update', upsert_res)
            socratautil.upsert_data(socrata_creds, log_entry, resource_id_pub_log)
            print(upsert_res)

        else:
            # trip data for this month not yet available
            print("trip data for last month not yet available.")
            
            #  update publication log
            upsert_res = { 'Errors' : 0, 'message' : 'No new trip data detected' , 'Rows Updated' : 0, 'Rows Created' : 0, 'Rows Deleted' : 0 }
            log_entry = socratautil.prep_pub_log(arrow.now(), 'bcycle_trip_update', upsert_res)
            socratautil.upsert_data(socrata_creds, log_entry, resource_id_pub_log)

            sys.exit()

except Exception as e:
    print('Failed to process bcycle trip data for {}'.format(arrow.now().format()))
    print(e)
    emailutil.send_email(ALERTS_DISTRIBUTION, 'BCycle Trip Update Failure', str(e), EMAIL['user'], EMAIL['password'])
    raise e