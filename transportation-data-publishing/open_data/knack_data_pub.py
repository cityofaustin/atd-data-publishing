'''
push data from Knack database to Socrata, ArcGIS Online, CSV

command example:
    python knack_data_pub.py signals -socrata -agol -csv
'''
import argparse
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
from util import datautil
from util import emailutil
from util import socratautil


def main(start_time):
    print('starting stuff now')
    
    try: 
        #  get data from Knack object or view
        kn = knackpy.Knack(
            obj=cfg[dataset]['obj'],
            scene=cfg[dataset]['scene'],
            view=cfg[dataset]['view'],
            ref_obj=cfg[dataset]['ref_obj'],
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key']
        )
        
        #  exclude records without primary key
        kn.data = datautil.filter_by_key_exists(
            kn.data,
            cfg[dataset]['primary_key']
        )

        if fetch_locations:
            #  optionally get location data from another knack view and merge with primary dataset
            locations = knackpy.Knack(
                obj=cfg['locations']['obj'],
                scene=cfg['locations']['scene'],
                view=cfg['locations']['view'],
                ref_obj=cfg['locations']['ref_obj'],
                app_id=KNACK_CREDENTIALS[app_name]['app_id'],
                api_key=KNACK_CREDENTIALS[app_name]['api_key']
            )
            
            #  merge location data to primary Knack object
            lat_field = cfg[dataset]['location_fields']['lat']
            lon_field = cfg[dataset]['location_fields']['lon']
            
            kn.data = datautil.merge_dicts(
                kn.data,
                locations.data,
                cfg[dataset]['location_join_field'],
                [lat_field, lon_field]
            )

        #  identify date fields for conversion from mills to unix
        date_fields_kn = [kn.fields[f]['label'] for f in kn.fields if kn.fields[f]['type'] in ['date_time', 'date']]
        
        if agol_pub:
            '''
            Delete existing AGOL features and publish current features
            TODO: detect changes instead of total replace
            '''
            agol_fail = 0
            
            lat_field = cfg[dataset]['location_fields']['lat']
            lon_field = cfg[dataset]['location_fields']['lon']

            token = agolutil.get_token(AGOL_CREDENTIALS)
            
            agol_payload = agolutil.build_payload(
                kn.data,
                lat_field=lat_field,
                lon_field=lon_field
            )

            del_response = agolutil.delete_features(
                cfg[dataset]['service_url'],
                token
            )
    
            add_response = agolutil.add_features(
                cfg[dataset]['service_url'],
                token,
                agol_payload
            )

            try:
                for res in add_response['addResults']:
                    if not res['success']:
                        raise KeyError
            
            except KeyError:
                logging.info('AGOL publication failed to upload. {}'.format(add_response))

                agol_fail += 1
                
                if agol_fail == 1:
                    #  alert on first failure, but continue processing
                    emailutil.send_email(
                        ALERTS_DISTRIBUTION,
                        'AGOL Feature Publish Failure: {}'.format(dataset),
                        str(add_response),
                        EMAIL['user'],
                        EMAIL['password']
                    )

        if socrata_pub:
            '''
            Create Socrata dataset instance
            '''
            socr = socratautil.Soda(
                resource,
                user=SOCRATA_CREDENTIALS['user'],
                password=SOCRATA_CREDENTIALS['password']
            )

            socr.get_data()
            socr.get_metadata()

            fieldnames = socr.fieldnames

            '''
            Normalize socrata and knack data for comparison
            '''
            if 'location' in fieldnames:
                fieldnames.remove('location') #  location field is reconstructed during publication
            date_fields_soc = socr.date_fields
            socr.data = datautil.iso_to_unix(socr.data, date_fields_soc)
            socr.data = datautil.lower_case_keys(socr.data)
            
            kn_socr = deepcopy(kn.data)
            kn_socr = datautil.mills_to_unix(kn_socr, date_fields_kn)
            kn_socr = datautil.lower_case_keys(kn_socr)

            '''
            We must stringify Knack values to compare them to Socrata data
            Socrata formats all data as strings except for timestamps
            '''
            kn_socr = datautil.stringify_key_values(kn_socr)
            kn_socr = datautil.remove_empty_entries(kn_socr)
            kn_socr = datautil.reduce_to_keys(kn_socr, fieldnames)

            cd_results = datautil.detect_changes(
                socr.data,
                kn_socr,
                cfg[dataset]['primary_key'].lower(),
                keys=fieldnames
            )

            if cd_results['new'] or cd_results['change'] or cd_results['delete']:
                
                socrata_payload = socratautil.create_payload(
                    cd_results,
                    cfg[dataset]['primary_key'].lower()
                )
                
                if 'location_fields' in cfg[dataset]:            
                    lat_field = cfg[dataset]['location_fields']['lat'].lower()
                    lon_field = cfg[dataset]['location_fields']['lon'].lower()
                    socrata_payload = socratautil.create_location_fields(
                        socrata_payload,
                        lat_field=lat_field,lon_field=lon_field
                    )

                upsert_response = socratautil.upsert_data(
                    SOCRATA_CREDENTIALS,
                    socrata_payload,
                    resource
                )

            else:
                #  mock upsert response when no change detected
                upsert_response = {
                    'Errors': 0,
                    'Rows Updated': 0,
                    'By RowIdentifier': 0,
                    'Rows Deleted': 0,
                    'By SID': 0,
                    'Rows Created': 0
                }

            if upsert_response.get('Errors') or upsert_response.get('error'):
                logging.error(upsert_response)
                logging.info(socrata_payload)
                emailutil.send_socrata_alert(
                    ALERTS_DISTRIBUTION,
                    resource,
                    upsert_response,
                    EMAIL['user'],
                    EMAIL['password']
                )

            #  get pub log payload
            log_payload = socratautil.pub_log_payload(
                script_id,  # id
                start_time.timestamp,  # start
                arrow.now().timestamp,  # end
                resource=resource,
                dataset=dataset
            )

            #  update pub log payload with data from upsert response
            log_payload = socratautil.handle_response(upsert_response, log_payload)
            logging.info(log_payload)

            #  upsert pub log payload
            pub_log_response = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                log_payload,
                pub_log_id
            )

        if write_csv:
            if 'csv_separator' in cfg[dataset]:
                sep = cfg[dataset]['csv_separator']
            else:
                sep = ','

            kn_csv = deepcopy(kn)
            kn_csv.data = datautil.mills_to_iso(kn_csv.data, date_fields_kn)
            file_name = '{}/{}.csv'.format(FME_DIRECTORY, dataset)
            kn_csv.to_csv(file_name, delimiter=sep)
            
        logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
        return True
        
    except Exception as e:
        print('Failed to process data for {}'.format(start_time))
        error_text = traceback.format_exc()
        email_subject = "Knack Data Pub Failure: {}".format(dataset)
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            email_subject,
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )
        
        logging.error(error_text)
        print(e)
        raise e


def cli_args():
    parser = argparse.ArgumentParser(
        prog='knack_data_pub.py',
        description='Publish Knack data to Socrata and ArcGIS Online'
    )

    parser.add_argument(
        'dataset',
        action="store",
        type=str,
        help='Name of the dataset that will be published.'
    )

    parser.add_argument(
        'app_name',
        action="store",
        choices=['data_tracker_prod', 'data_tracker_test', 'visitor_sign_in_prod'],
        type=str,
        help='Name of the knack application that will be accessed'
    )

    parser.add_argument(
        '-agol',
        action='store_true',
        default=False,
        help='Publish to ArcGIS Online.'
    )

    parser.add_argument(
        '-socrata',
        action='store_true',
        default=False,
        help='Publish to Socrata, AKA City of Austin Open Data Portal.'
    )
    
    parser.add_argument(
        '-csv',
        action='store_true',
        default=False,
        help='Write output to csv.'
    )
    
    args = parser.parse_args()
    
    return(args)

if __name__ == '__main__':
    #  parse command-line arguments
    args = cli_args()
    dataset = args.dataset
    app_name = args.app_name
    socrata_pub = args.socrata    
    agol_pub = args.agol
    write_csv = args.csv
    script_name = __file__.split('.')[0]
    script_id = '{}_{}'.format(script_name, dataset)
    resource = cfg[dataset]['socrata_resource_id']
    now = arrow.now()
    pub_log_id = cfg['publication_log']['socrata_resource_id']

    script = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script}_{dataset}.log'
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info( 'args: {}'.format( str(args) ) )
    logging.info('START AT {}'.format(str(now)))
    
    if 'fetch_locations' in cfg[dataset]:
        fetch_locations = cfg[dataset]['fetch_locations']
    else:
        fetch_locations = False

    results = main(now)

print(results)



