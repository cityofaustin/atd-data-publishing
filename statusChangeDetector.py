#  pass date and params in function
#  logging and stuff

import pymssql
import arrow
import requests
import json
from secrets import KITS_CREDENTIALS
import updateLogs

import pdb

PUBLISHED_DATA_URL = 'https://data.austintexas.gov/resource/utgi-umz5.json'

now = arrow.now('America/Chicago')

def fetch_kits_data():

    conn = pymssql.connect(
        server=KITS_CREDENTIALS['server'],
        user=KITS_CREDENTIALS['user'],
        password=KITS_CREDENTIALS['password'],
        database=KITS_CREDENTIALS['database']
    )

    cursor = conn.cursor(as_dict=True)

    search_string = '''
        SELECT i.INTID
            , e.INTNAME
            , e.DATETIME as INTSTATUSDATETIME
            , e.STATUS as INTSTATUS
            , i.POLLST as POLLSTATUS
            , e.OPERATION as OPERATIONSTATE
            , e.PLANID
            , i.STREETN1
            , i.STREETN2
            , i.ASSETNUM
            , i.LATITUDE
            , i.LONGITUDE
            FROM [KITS].[INTERSECTION] i
            LEFT OUTER JOIN [KITS].[INTERSECTIONSTATUS] e
            ON i.[INTID] = e.[INTID]
            ORDER BY i.[INTID] ASC
    '''

    cursor.execute(search_string)  

    return cursor.fetchall()


def fetch_published_data():
    try:
        res = requests.get(PUBLISHED_DATA_URL, verify=False)

    except requests.exceptions.HTTPError as e:
        raise e

    return res.json()


def group_data(dataset, key):
    grouped_data = {}
    
    for row in dataset:
        new_key = str(row[key])
        grouped_data[new_key] = row

    return grouped_data

def detect_changes(new, old):
    changed = 0
    not_changed = 0
    new_id = 0
    
    for record in new:
        lookup = str(new[record]['INTID'])

        if lookup in old:
            new_status = str(new[record]['INTSTATUS'])
            old_status = old[lookup]['intstatus']
            
            if new_status == old_status:
                not_changed += 1
            
            else:
                changed += 1
            
        else:
            new_id+=1

    return [changed, not_changed, new_id]

def main(date_time):
    try:
        new_data = fetch_kits_data()
        new_data_grouped = group_data(new_data, 'INTID')
                
        old_data = fetch_published_data()
        old_data_grouped = group_data(old_data, 'intid')

        change_detected = detect_changes(new_data_grouped, old_data_grouped)

        log_update = [date_time, change_detected[0], change_detected[1], change_detected[2]]
        
        updateLogs.log_signal_status_etl(date_time, log_update)

        return log_update
 
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        raise e
 
delta = main(now)

print(arrow.now('America/Chicago') - now)
