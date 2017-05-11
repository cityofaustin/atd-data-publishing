'''
push data from Knack database to Socrata, ArcGIS Online, CSV

command example:
    python knack_data_pub.py signals -socrata -agol -csv
    python knack_data_pub.py quote_of_the_week -github
'''

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import argparse
from copy import deepcopy
import logging
import arrow
import pdb
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
import github_helpers
import secrets
from config import config


def main(date_time):
    print('starting stuff now')

    try:       
        field_data = knack_helpers.get_fields(knack_objects, knack_creds)
        
        if knack_view:
            knack_data = knack_helpers.get_data(knack_scene, knack_view, knack_creds)

        if not knack_view:
            knack_data = knack_helpers.get_object_data(knack_objects[0], knack_creds)
            
        knack_data = knack_helpers.parse_data(knack_data, field_data, convert_to_unix=True, include_ids=include_ids, id_outfield='SOURCE_DB_ID')
        field_names = data_helpers.unique_keys(knack_data)

        #  stringify values for later comparison against socrata JSON
        knack_data = data_helpers.stringify_key_values(knack_data)

        if agol_pub:
            knack_data_mills = data_helpers.unix_to_mills(deepcopy(knack_data))            
            token = agol_helpers.get_token(agol_creds)
            agol_payload = agol_helpers.build_payload(knack_data_mills)
            del_response = agol_helpers.delete_features(service_url, token)
            
            if 'deleteResults' in del_response:
                del_results = agol_helpers.parse_response(del_response, "delete")
                logging.info( "{} items deleted, {} items failed to delete".format(del_results['success'], del_results['fail']) )

            else:
                logging.info('no arcgis online features delete')

            add_response = agol_helpers.add_features(service_url, token, agol_payload)
            
            if 'addResults' in add_response:
                add_results = agol_helpers.parse_response(add_response, "add")
            
            else:
                logging.info('no arcgis online features add')

        if socrata_pub:
            socrata_data = socrata_helpers.get_private_data(socrata_creds, socrata_resource_id)
            socrata_data = data_helpers.upper_case_keys(socrata_data)
            socrata_data = data_helpers.stringify_key_values(socrata_data)
            socrata_data = data_helpers.iso_to_unix(socrata_data, replace_tz=True)

            cd_results = data_helpers.detect_changes(socrata_data, knack_data, config[dataset]['primary_key'], keys=field_names)
            logging.info( 'socrata change detection results: {}'.format(cd_results) )

            if cd_results['new'] or cd_results['change'] or cd_results['delete']:
                socrata_payload = socrata_helpers.create_payload(cd_results, config[dataset]['primary_key'])
                socrata_payload = socrata_helpers.create_location_fields(socrata_payload)

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

            log_payload = socrata_helpers.prep_pub_log(date_time, '{}_update'.format(dataset), upsert_response)
            pub_log_response = socrata_helpers.upsert_data(secrets.SOCRATA_CREDENTIALS, log_payload, pub_log_id)

            logging.info(pub_log_response)

        if write_csv:
            knack_data = data_helpers.unix_to_iso(knack_data)
            file_name = '{}/{}.csv'.format(csv_dest, dataset)
            data_helpers.write_csv(knack_data, file_name=file_name)

        if github_pub:
            print('commit to github')

            git_data = data_helpers.unix_to_mills(deepcopy(knack_data))
            payload = data_helpers.write_csv(git_data, in_memory=True)
            git_auth = github_helpers.create_auth_tuple(github_creds)
            repo_data = github_helpers.get_file(repo_url, git_path, branch, git_auth)
            sha = repo_data['sha']
            git_response = github_helpers.commit_file(repo_url, git_path, branch, payload, 'update_quote_of_week', sha, git_auth, existing_file=repo_data)
            
        logging.info('END AT {}'.format(str( arrow.now().timestamp) ))
        return "done"
        # return log_payload

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e


def cli_args():
    parser = argparse.ArgumentParser(prog='knack_data_pub.py', description='Publish Knack data to Socrata and ArcGIS Online')
    parser.add_argument('dataset', action="store", type=str, help='Name of the dataset that will be published.')
    parser.add_argument('-agol', action='store_true', default=False, help='Publish to ArcGIS Online.')
    parser.add_argument('-socrata', action='store_true', default=False, help='Publish to Socrata, AKA City of Austin Open Data Portal.')
    parser.add_argument('-csv', action='store_true', default=False, help='Write output to csv.')
    parser.add_argument('-github', action='store_true', default=False, help='Commit data to github.')
    args = parser.parse_args()
    
    return(args)


if __name__ == '__main__':
    
    #  parse command-line arguments
    args = cli_args()
    dataset = args.dataset
    socrata_pub = args.socrata    
    agol_pub = args.agol
    write_csv = args.csv
    github_pub = args.github
    
    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    #  init logging 
    #  with one logfile per dataset per day
    logfile = './log/{}_{}.log'.format(dataset, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info( 'args: {}'.format( str(args) ) )
    logging.info('START AT {}'.format(str(now)))
    
    #  set global variables from config data
    primary_key = config[dataset]['primary_key']    
    knack_view = config[dataset]['view'] 
    knack_scene = config[dataset]['scene']
    knack_objects = config[dataset]['objects']
    service_url = config[dataset]['service_url']
    socrata_resource_id = config[dataset]['socrata_resource_id']
    pub_log_id = config[dataset]['pub_log_id']
    include_ids = config[dataset]['include_ids']

    if 'repo_url' in config[dataset]:
        repo_url = config[dataset]['repo_url']
    
    if 'branch' in config[dataset]:
        branch = config[dataset]['branch']

    if 'git_path' in config[dataset]:
        git_path = config[dataset]['git_path']

    knack_creds = secrets.KNACK_CREDENTIALS
    agol_creds = secrets.AGOL_CREDENTIALS
    socrata_creds = secrets.SOCRATA_CREDENTIALS
    csv_dest = secrets.FME_DIRECTORY  
    github_creds = secrets.GITHUB_CREDENTIALS  

    results = main(now)


print(results)