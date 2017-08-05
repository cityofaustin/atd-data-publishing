#  sync master list of signal requests with open data portal
#  the only thing that distinguishes this script from
#  your average knack_data_pub is that data from two views
#  (phb request and signal requests) is merged to one dict

from copy import deepcopy
import logging
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import agolutil
from util import socratautil
from util import emailutil
from util import datautil

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

#  init logging with one logfile per dataset per day
logfile = '{}/sig_req_rank_pub_{}.log'.format(LOG_DIRECTORY, now_s)
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
socrata_pub_log_id = 'n5kp-f8k4'


def main(date_time):
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

        socr.get_metadata()
        fieldnames_socr = socr.fieldnames

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
            
        log_payload = socratautil.prep_pub_log(date_time, 'signal_request_master_list', upsert_response)

        pub_log_response = socratautil.upsert_data(SOCRATA_CREDENTIALS, log_payload, socrata_pub_log_id)

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
