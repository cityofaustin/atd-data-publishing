#  download quotes of the week from knack
#  find the latest
#  commit it to github

import arrow
import requests
import data_helpers
import knack_helpers
import csv
from io import StringIO
import github_helpers
import secrets

import pdb


#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_67'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}


#  GIT CONFIG
GIT_PARAMS = {
    'REPO_URL' : 'https://api.github.com/repositories/55646931/contents',
    'BRANCH' : 'gh-pages',
    'PATH' : 'components/data/quote_of_the_week.csv'
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
            fields = knack_helpers.get_all_fields(obj, KNACK_PARAMS)
            
            #  assign field metadata to 'raw' field name
            field_list = {}

            for field in fields:
                field_list[field['key'] + '_raw'] = field

            #  update knack params with list of all field names
            KNACK_PARAMS['FIELD_NAMES'] = knack_helpers.create_label_list(fields)
            
            #  get knack object data
            data = knack_helpers.get_object_data(obj, KNACK_PARAMS)

            #  parse data
            data = knack_helpers.parse_data(data, field_list, KNACK_PARAMS, convert_to_unix=True)
            
            #  prepare dates for the internet
            data = data_helpers.unix_to_mills(data)
            
            payload = data_helpers.write_csv(data, in_memory=True)

            git_auth = github_helpers.create_auth_tuple(secrets.GITHUB_CREDENTIALS)

            repo_data = github_helpers.get_file(GIT_PARAMS['REPO_URL'], GIT_PARAMS['PATH'], 'gh-pages', git_auth)

            GIT_PARAMS['sha'] = repo_data['sha']

            git_response = github_helpers.commit_file(GIT_PARAMS['REPO_URL'], GIT_PARAMS['PATH'], GIT_PARAMS['BRANCH'], payload, 'update_quote_of_week', GIT_PARAMS['sha'], git_auth, existing_file=repo_data)
            
        return git_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e
 

results = main(then)

print('Elapsed time: {}'.format(str(arrow.now() - then)))

print(results)