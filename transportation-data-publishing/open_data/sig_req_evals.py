'''
Sync master list of signal requests with open data portal
The only thing that distinguishes this script from
Your average knack_data_pub is that data from two views
(phb request and signal requests) are merged to one payload dict
'''
from copy import deepcopy
import logging
import os
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *
from util import agolutil
from util import socratautil
from util import emailutil
from util import datautil

now = arrow.now()

script = os.path.basename(__file__).replace('.py', '.log')
logfile = f'{LOG_DIRECTORY}/{script}'
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

#  KNACK CONFIG
primary_key = 'ATD_EVAL_ID'
knack_creds = KNACK_CREDENTIALS
app_name = 'data_tracker_prod'

knack_params_traffic = {  
    'ref_obj' : ['object_13', 'object_27'],
    'scene' : 'scene_175',
    'view' : 'view_908',
}

knack_params_phb = {  
    'ref_obj' : ['object_13', 'object_26'],
    'scene' : 'scene_175',
    'view' : 'view_911'
}

knack_params_req_locations = {  
    'ref_obj' : ['object_11', 'object_13'],
    'scene' : 'scene_175',
    'view' : 'view_923'
}

#  SOCRATA CONFIG
socrata_resource_id = 'f6qu-b7zb'
pub_log_id = cfg['publication_log']['socrata_resource_id']

script_name = __file__.split('.')[0]


def main(start_time):
    print('starting stuff now')

    try:       
        #  get phb requests
        kn_phb = knackpy.Knack(
            scene=knack_params_phb['scene'],
            view=knack_params_phb['view'],
            ref_obj=knack_params_phb['ref_obj'],
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key']
        )

        #  get signal requests
        kn_traffic = knackpy.Knack(
            scene=knack_params_traffic['scene'],
            view=knack_params_traffic['view'],
            ref_obj=knack_params_traffic['ref_obj'],
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key']
        )
        
        #  get location data
        locations = knackpy.Knack(
                scene=knack_params_req_locations['scene'],
                view=knack_params_req_locations['view'],
                ref_obj=knack_params_req_locations['ref_obj'],
                app_id=KNACK_CREDENTIALS[app_name]['app_id'],
                api_key=KNACK_CREDENTIALS[app_name]['api_key']
        )

        #  merge traffic and phb request data
        knack_data_master = kn_traffic.data + kn_phb.data
    
        fieldnames_kn = list(set( kn_phb.fieldnames + kn_traffic.fieldnames))

        #  join location data to request data
        knack_data_master = datautil.merge_dicts(
                knack_data_master,
                locations.data,
                'REQUEST_ID',
                ['LOCATION_latitude', 'LOCATION_longitude']
            )

        #  format data for comparison with socrata
        knack_data_master = datautil.lower_case_keys(knack_data_master)
        knack_data_master = datautil.stringify_key_values(knack_data_master)
        knack_data_master = datautil.remove_empty_entries(knack_data_master)

        #  get published request data from Socrata
        socr = socratautil.Soda(
                socrata_resource_id,
                user=SOCRATA_CREDENTIALS['user'],
                password=SOCRATA_CREDENTIALS['password']
            )

        socr.get_data()
        socr.get_metadata()
        fieldnames_socr = socr.fieldnames
        
        if 'location' in fieldnames_socr:
            fieldnames_socr.remove('location') #  fieldname is reconstructed during publicaiton
        
        #  compare knack data to socrata data
        cd_results = datautil.detect_changes(socr.data, knack_data_master, primary_key.lower(), keys=fieldnames_socr)
        
        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            socrata_payload = socratautil.create_payload(cd_results, primary_key.lower())

            socrata_payload = socratautil.create_location_fields(
                socrata_payload,
                lat_field='location_latitude',
                lon_field='location_longitude'
                )

            logging.info(socrata_payload)

        else:
            socrata_payload = []
        
        upsert_response = socratautil.upsert_data(SOCRATA_CREDENTIALS, socrata_payload, socrata_resource_id)
        logging.info(upsert_response)

        if 'Errors' in upsert_response:
            if upsert_response['Errors']:
                emailutil.send_socrata_alert(ALERTS_DISTRIBUTION, socrata_resource_id, upsert_response, EMAIL['user'], EMAIL['password'])

        else:
            emailutil.send_socrata_alert(ALERTS_DISTRIBUTION, socrata_resource_id, upsert_response, EMAIL['user'], EMAIL['password'])

        #  get pub log payload
        log_payload = socratautil.pub_log_payload(
            script_name,  #  id
            start_time.timestamp,  #  start
            arrow.now().timestamp,  #  end
            resource=socrata_resource_id,
            dataset='Traffic and Pedestrian Signal Requests'
        )

        #  update pub log payload with data from upsert response
        log_payload = socratautil.handle_response(upsert_response, log_payload)

        pub_log_response = socratautil.upsert_data(
            SOCRATA_CREDENTIALS,
            log_payload,
            pub_log_id
        )

        logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
        return log_payload

    except Exception as e:
        error_text = traceback.format_exc()
        email_subject = "Signal Requests Data Pub Failure"
        emailutil.send_email(ALERTS_DISTRIBUTION, socrata_resource_id, error_text, EMAIL['user'], EMAIL['password'])
        logging.error(error_text)
        print(e)
        raise e

results = main(now)

print(results)
