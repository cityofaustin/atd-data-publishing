#  download quotes of the week from knack
#  find the latest
#  commit it to github

import arrow
import requests
import data_helpers
import knack_helpers
import csv
from StringIO import StringIO
import github_helpers
import secrets

import pdb

REPO_URL_GITHUB = 'https://api.github.com/repos/cityofaustin/transportation-logs/contents/'
DATA_URL_GITHUB = 'https://raw.githubusercontent.com/cityofaustin/transportation-logs/master/'
DATASET_FIELDNAMES = ['date_time', 'socrata_errors', 'socrata_updated', 'socrata_created', 'socrata_deleted', 'no_update', 'update_requests', 'insert_requests', 'delete_requests', 'not_processed','response_message']


#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_67'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}


#  GIT CONFIG
GIT_PARAMS = {
    'REPO_URL' : 'https://github.com/cityofaustin/transportation',
    'BRANCH' : 'gh-pages'
}

then = arrow.now()


def main(date_time):
    print('starting stuff now')

    try:
        url = 'https://raw.githubusercontent.com/cityofaustin/transportation/gh-pages/components/data/quote_of_the_week.csv'
        

        #  get new quote data from Knack database
        #  this is where the user maintains the quotes
        for obj in KNACK_PARAMS['REFERENCE_OBJECTS']:
            
            #  get field metadata
            fields = knack_helpers.GetAllFields(obj, KNACK_PARAMS)
            
            #  assign field metadata to 'raw' field name
            field_list = {}

            for field in fields:
                field_list[field['key'] + '_raw'] = field

            #  update knack params with list of all field names
            KNACK_PARAMS['FIELD_NAMES'] = knack_helpers.CreateFieldLabelList(fields)
            
            #  get knack object data
            data = knack_helpers.GetObjectData(obj, KNACK_PARAMS)

            #  parse data
            data = knack_helpers.ParseData(data, field_list, KNACK_PARAMS, convert_to_unix=True)
            
            #  prepare dates for the internet
            data = data_helpers.ConvertUnixToMills(data)
            
            payload = data_helpers.WriteToCSV(data, stringify_only=True)

            git_auth = github_helpers.CreateAuthTuple(secrets.GITHUB_CREDENTIALS['transportation'])

            pdb.set_trace()

            git_response = github_helpers.CommitFile(GIT_PARAMS['REPO_URL'], GIT_PARAMS['BRANCH'], payload, 'update_quote_of_week', git_auth)
            
            pdb.set_trace()
        return 'youve got the newest now'

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e
 

results = main(then)

print('Elapsed time: {}'.format(str(arrow.now() - then)))


