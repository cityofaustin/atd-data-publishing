
#  sync master list of signal requests with open data portal

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from copy import deepcopy
import logging
import pdb
import arrow
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
import secrets

log_directory = secrets.LOG_DIRECTORY

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

#  init logging with one logfile per dataset per day
logfile = '{}/sig_req_rank_pub_{}.log'.format(log_directory, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

#  KNACK CONFIG
primary_key = 'ATD_EVAL_ID'

knack_creds = secrets.KNACK_CREDENTIALS

knack_params_traffic = {  
    'objects' : ['object_13', 'object_27'],
    'scene' : '175',
    'view' : '908',
}

knack_params_phb = {  
    'objects' : ['object_13', 'object_26'],
    'scene' : '175',
    'view' : '911'
}

knack_params_req_locations = {  
    'objects' : ['object_11', 'object_13'],
    'scene' : '175',
    'view' : '923'
}

#  SOCRATA CONFIG
socrata_resource_id = 'f6qu-b7zb'
socrata_pub_log_id = 'n5kp-f8k4'




def main(date_time):
    print('starting stuff now')

    try:       
        #  get and parse phb eval data
        field_dict = knack_helpers.get_fields(knack_params_phb['objects'], knack_creds)

        knack_data_phb = knack_helpers.get_data(knack_params_phb['scene'], knack_params_phb['view'], knack_creds)

        knack_data_phb = knack_helpers.parse_data(knack_data_phb, field_dict, convert_to_unix=True)

        knack_data_phb = data_helpers.stringify_key_values(knack_data_phb)
        
        knack_data_phb_mills = data_helpers.unix_to_mills(deepcopy(knack_data_phb))

        #  get and parse traffic eval data
        field_dict = knack_helpers.get_fields(knack_params_traffic['objects'], knack_creds)

        knack_data_traffic = knack_helpers.get_data(knack_params_traffic['scene'], knack_params_traffic['view'], knack_creds)

        knack_data_traffic = knack_helpers.parse_data(knack_data_traffic, field_dict, convert_to_unix=True)

        field_names = data_helpers.unique_keys(knack_data_traffic)

        knack_data_traffic = data_helpers.stringify_key_values(knack_data_traffic)
        
        knack_data_traffic_mills = data_helpers.unix_to_mills(deepcopy(knack_data_traffic))
        
        knack_data_master = knack_data_traffic_mills + knack_data_phb_mills
        
        #  get and parse location info
        field_dict = knack_helpers.get_fields(knack_params_req_locations['objects'], knack_creds)

        knack_data_req_loc = knack_helpers.get_data(knack_params_req_locations['scene'], knack_params_req_locations['view'], knack_creds)

        knack_data_req_loc = knack_helpers.parse_data(knack_data_req_loc, field_dict, convert_to_unix=True)

        knack_data_req_loc = data_helpers.stringify_key_values(knack_data_req_loc)

        #  append location info to eval data dicts
        knack_data_master = data_helpers.merge_dicts(knack_data_master, knack_data_req_loc, 'REQUEST_ID', ['LATITUDE', 'LONGITUDE'])

        #  get published request data from Socrata and compare to Knack database
        socrata_data = socrata_helpers.get_private_data(secrets.SOCRATA_CREDENTIALS, socrata_resource_id)

        socrata_data = data_helpers.upper_case_keys(socrata_data)
        
        socrata_data = data_helpers.stringify_key_values(socrata_data)
        
        socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)
        
        cd_results = data_helpers.detect_changes(socrata_data, knack_data_master, primary_key, keys=field_names)

        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            socrata_payload = socrata_helpers.create_payload(cd_results, primary_key)

            socrata_payload = socrata_helpers.create_location_fields(socrata_payload)

            logging.info(socrata_payload)

        else:
            socrata_payload = []
        
        socrata_payload = data_helpers.lower_case_keys(socrata_payload)

        socrata_payload = data_helpers.unix_to_iso(socrata_payload)

        upsert_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, socrata_payload, socrata_resource_id)
        
        logging.info(upsert_response)

        if 'error' in upsert_response:
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, socrata_resource_id, upsert_response)
            
        elif upsert_response['Errors']:
            email_helpers.send_socrata_alert(secrets.ALERTS_DISTRIBUTION, socrata_resource_id, upsert_response)

        log_payload = socrata_helpers.prep_pub_log(date_time, 'signal_request_master_list', upsert_response)

        pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, socrata_pub_log_id)

        logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
        return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        logging.error(str(e))
        print(e)
        raise e

results = main(now)

print(results)
