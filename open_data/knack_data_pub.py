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
from config.config import cfg
from config.secrets import *
from util import agolutil
from util import datautil
from util import emailutil
from util import socratautil


def main(date_time):
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
            agol_fail = 0
            #  delete existing AGOL features and publish current features
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
            
            for res in add_response['addResults']:
                if 'success' in res:
                    if res['success']:
                        continue
                
                logging.info('AGOL publicatoin failed to upload. {}'.format(
                    res
                ))

                agol_fail += 1
                
                if agol_fail == 1:
                    
                    #  alert on first failure, but continue processing
                    emailutil.send_email(
                        ALERTS_DISTRIBUTION,
                        'AGOL Feature Publish Failure: {}'.format(dataset),
                        str(res),
                        EMAIL['user'],
                        EMAIL['password']
                    )

        if socrata_pub:
            #  Create Socrata dataset instance
            socr = socratautil.Soda(
                cfg[dataset]['socrata_resource_id'],
                user=SOCRATA_CREDENTIALS['user'],
                password=SOCRATA_CREDENTIALS['password']
            )
            
            #  Get metadata and fieldnames
            #  Fieldnames will be used to filter fields in knack dataset
            socr.get_metadata()
            fieldnames = socr.fieldnames
            if 'location' in fieldnames:
                fieldnames.remove('location') #  fieldname is reconstructed during publicaiton
            date_fields_soc = socr.date_fields
            socr.data = datautil.iso_to_unix(socr.data, date_fields_soc)
            socr.data = datautil.lower_case_keys(socr.data)
            
            #  Format kack data and reduce to keys that are in socrata  
            kn_socr = deepcopy(kn.data)
            kn_socr = datautil.mills_to_unix(kn_socr, date_fields_kn)
            kn_socr = datautil.lower_case_keys(kn_socr)
            #  We must stringify Knack values to compare them to Socrata data
            #  Socrata formats all data as strings except for timestamps
            kn_socr = datautil.stringify_key_values(kn_socr)
            kn_socr = datautil.remove_empty_entries(kn_socr)
            kn_socr = datautil.reduce_to_keys(kn_socr, fieldnames)

            #  compare old and new data            
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
                    cfg[dataset]['socrata_resource_id']
                )

            else:
                #  mock upsert response
                upsert_response = {
                    'Errors': 0,
                    'Rows Updated': 0,
                    'By RowIdentifier': 0,
                    'Rows Deleted': 0,
                    'By SID': 0,
                    'Rows Created': 0
                }

            logging.info(upsert_response)

            if upsert_response['Errors']:
                emailutil.send_socrata_alert(
                    ALERTS_DISTRIBUTION,
                    socrata_resource_id,
                    upsert_response
                )

            log_payload = socratautil.prep_pub_log(
                date_time,
                '{}_update'.format(dataset),
                upsert_response
            )

            pub_log_response = socratautil.upsert_data(
                SOCRATA_CREDENTIALS,
                log_payload,
                cfg[dataset]['pub_log_id']
            )

            logging.info(pub_log_response)

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
        print('Failed to process data for {}'.format(date_time))
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
    
    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    #  with one logfile per dataset per day
    cur_dir = os.path.dirname(__file__)
    logfile = '{}/{}_{}.log'.format(LOG_DIRECTORY, dataset, now_s)
    log_path = os.path.join(cur_dir, logfile)
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logging.info( 'args: {}'.format( str(args) ) )
    logging.info('START AT {}'.format(str(now)))
    
    if 'fetch_locations' in cfg[dataset]:
        fetch_locations = cfg[dataset]['fetch_locations']
    else:
        fetch_locations = False

    if 'repo_url' in cfg[dataset]:
        repo_url = cfg[dataset]['repo_url']
    
    if 'branch' in cfg[dataset]:
        branch = cfg[dataset]['branch']

    if 'git_path' in cfg[dataset]:
        git_path = cfg[dataset]['git_path']

    results = main(now)

print(results)




