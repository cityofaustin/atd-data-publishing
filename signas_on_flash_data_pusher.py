#  enable request verification
#  logging and stuff
#  fieldnames! e.g. atd_intersection_id
#  dodgy error handling in change detection
#  use ATD intersection ID as row identifier

import pymssql
import arrow
import requests
import json
import github_updater
from secrets import KITS_CREDENTIALS
from secrets import SOCRATA_CREDENTIALS

import pdb

REPO_URL_GITHUB = 'https://api.github.com/repos/cityofaustin/transportation-logs/contents/'
DATA_URL_GITHUB = 'https://raw.githubusercontent.com/cityofaustin/transportation-logs/master/'
LOGFILE_FIELDNAMES = ['date_time', 'socrata_errors', 'socrata_updated', 'socrata_created', 'socrata_deleted', 'no_update', 'update_requests', 'insert_requests', 'delete_requests', 'not_processed','response_message']
SOCRATA_ENDPOINT = 'https://data.austintexas.gov/resource/5zpr-dehc.json'
IGNORE_INTESECTIONS =['959']

then = arrow.now()
logfile_filename = 'logs/signals-on-flash/{}.csv'.format(then.format('YYYY-MM-DD'))



def fetch_kits_data():
    print('fetch kits data')
    conn = pymssql.connect(
        server=KITS_CREDENTIALS['server'],
        user=KITS_CREDENTIALS['user'],
        password=KITS_CREDENTIALS['password'],
        database=KITS_CREDENTIALS['database']
    )

    cursor = conn.cursor(as_dict=True)

    search_string = '''
        SELECT i.INTID as intid
            , e.INTNAME as intname
            , e.DATETIME as intstatusdatetime
            , e.STATUS as intstatus
            , i.POLLST as pollstatus
            , e.OPERATION as operationstate
            , e.PLANID as planid
            , i.STREETN1 as streetn1
            , i.STREETN2 as streetn2
            , i.ASSETNUM as assetnum
            , i.LATITUDE as latitude
            , i.LONGITUDE as longitude
            FROM [KITS].[INTERSECTION] i
            LEFT OUTER JOIN [KITS].[INTERSECTIONSTATUS] e
            ON i.[INTID] = e.[INTID]
            ORDER BY i.[INTID] ASC
    '''

    cursor.execute(search_string)  

    return cursor.fetchall()



def fetch_published_data():
    print('fetch published data')
    try:
        res = requests.get(SOCRATA_ENDPOINT, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()



def reformat_sql_data(dataset):
    print('reformat data')
    
    reformatted_data = []
    
    for row in dataset:        
        formatted_row = {}

        for key in row:
            new_key = str(key)
            new_value = str(row[key])
            formatted_row[new_key] = new_value
        
        reformatted_data.append(formatted_row)

    return reformatted_data



def group_data(dataset, key):
    print('group data')
    grouped_data = {}
    
    for row in dataset:
        new_key = str(row[key])
        grouped_data[new_key] = row

    return grouped_data



def detect_changes(new, old):
    print('detect changes')

    upsert = []  #  see https://dev.socrata.com/publishers/upsert.html
    not_processed = []
    no_update = 0  
    insert = 0
    update= 0
    delete = 0    

    for record in new:  #  compare KITS to socrata data
        lookup = str(new[record]['intid'])

        if lookup in IGNORE_INTESECTIONS:
            continue
            
        if lookup in old:
            new_status = str(new[record]['intstatus'])
            
            try:
                old_status = str(old[lookup]['intstatus'])

            except:
                not_processed.append(new[record]['intid'])
                continue
            
            if new_status == old_status:
                no_update += 1
            
            else:
                update += 1
                new[record]['intstatusprevious'] = old_status
                upsert.append(new[record])
            
        else:
            insert += 1

            upsert.append(new[record])

    for record in old:  #  compare socrata to KITS to idenify deleted records
        lookup = old[record]['intid']
        
        if lookup not in new:
            delete += 1

            upsert.append({ 
                'intid': lookup,
                ':deleted': True
            })

    return { 
        'upsert': upsert,
        'not_processed': not_processed,
        'insert': insert,
        'update': update,
        'no_update':  no_update,
        'delete': delete
    }

def prepare_socrata_payload(upsert_data):
    now = arrow.now()

    for row in upsert_data:
        row['processeddatetime']  = now.format('YYYY-MM-DD HH:mm:ss')
        row['recordid'] = '{}_{}'.format(row['intid'], str(now.timestamp))

    return upsert_data



def upsert_open_data(payload):
    try:
        auth = (SOCRATA_CREDENTIALS['user'], SOCRATA_CREDENTIALS['password'])

        json_data = json.dumps(payload)

        res = requests.post(SOCRATA_ENDPOINT, data=json_data, auth=auth, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e
    
    return res.json()



def package_log_data(date, changes, response):

    date = date.format('YYYY-MM-DD HH:mm:ss')
   
    if 'error' in response.keys():
        response_message = response['message']
        socrata_errors = ''
        socrata_updated = ''
        socrata_created = ''
        socrata_deleted = ''

    else:
        socrata_errors = response['Errors']
        socrata_updated = response['Rows Updated']
        socrata_created = response['Rows Created']
        socrata_deleted = response['Rows Deleted']
        response_message = ''

    no_update = changes['no_update']
    update_requests = changes['update']
    insert_requests = changes['insert']
    delete_requests = changes['delete']
    if changes['not_processed']:
        not_processed = str(changes['not_processed'])
    else:
        not_processed = ''
     
    return [date, socrata_errors, socrata_updated, socrata_created, socrata_deleted, no_update, update_requests, insert_requests, delete_requests, not_processed, response_message]
    

    
def main(date_time):
    print('starting stuff now')

    try:
        new_data = fetch_kits_data()
        new_data_reformatted = reformat_sql_data(new_data)
        new_data_grouped = group_data(new_data_reformatted, 'intid')
                
        old_data = fetch_published_data()
        old_data_grouped = group_data(old_data, 'intid')

        change_detection_results = detect_changes(new_data_grouped, old_data_grouped)

        socrata_payload = prepare_socrata_payload(change_detection_results['upsert'])
        
        socrata_response = upsert_open_data(socrata_payload)

        logfile_data = package_log_data(date_time, change_detection_results, socrata_response)

        github_updater.update_github_repo(date_time, logfile_data, LOGFILE_FIELDNAMES, REPO_URL_GITHUB, DATA_URL_GITHUB, logfile_filename)

        return {
            'res': socrata_response,
            'payload': socrata_payload,
            'logfile': logfile_data
        }
    
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e
 

results = main(then)

print(results['res'])
print('Elapsed time: {}'.format(str(arrow.now() - then)))


