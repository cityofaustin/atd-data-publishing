'''
Extract radar traffic count data from KITS database and publish
new records to City of Austin Open Data Portal.
'''
import hashlib
import os
import pdb
import sys
import traceback

import arrow
import knackpy

import _setpath
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil
from tdutils import kitsutil
from tdutils import socratautil   


def my_round(x, base=15):
    # https://stackoverflow.com/questions/2272149/round-to-5-or-other-number-in-python
    return int(base * round(float(x)/base))


def get_timebin(minute, hour):
    '''
    Round an arbitrary minue/hour to the nearest 15 minutes
    '''
    minute = my_round(minute)
    hour_offset = 0
    
    if minute == 60:
        minute = 0
        hour_offset = 1

    timebin = '{}:{}'.format(hour + hour_offset, minute)

    return arrow.get(timebin,'H:m').format('HH:mm')


def get_direction(lane):
    if 'SB' in lane:
        return 'SB'
    elif 'NB' in lane:
        return 'NB'
    elif 'EB' in lane:
        return 'EB'
    elif 'WB' in lane:
        return 'WB'
    else:
        return None


def main():

    #  get most recent traffic count record from socrata
    socrata_data = socratautil.Soda(
        resource=socrata_resource,
        soql = {
            '$order':'curdatetime desc',
            '$limit':1
        }
    )

    socrata_data = socrata_data.data

    kits_query_recent =   '''
        SELECT TOP (1) DETID as det_id
        ,CURDATETIME as dettime
        ,DETNAME as lane
        ,VOLUME as vol
        ,SPEED as spd
        FROM [KITS].[SYSDETHISTORYRM]
        ORDER BY CURDATETIME DESC
        '''   

    kits_data_recent = kitsutil.data_as_dict(
        KITS_CREDENTIALS,
        kits_query_recent
    )

    for record in kits_data_recent:
        new_date = arrow.get(record['dettime'],'US/Central')
        record['dettime'] = new_date.timestamp

    if replace:
 
        kits_query =  '''
            SELECT i.DETID as detid
            ,i.CURDATETIME as curdatetime
            ,i.VOLUME as volume
            ,i.SPEED as speed
            ,i.INTNAME as intname
            ,i.OCCUPANCY as occupancy
            ,e.INTID as int_id
            ,e.DETSN as detname
            FROM [KITS].[SYSDETHISTORYRM] i
            LEFT OUTER JOIN [KITS].[DETECTORSRM] e
            ON i.[DETID] = e.[DETID]
            ORDER BY CURDATETIME DESC
        '''
    
    # send new data if the socrata data is behind KITS data
    elif socrata_data[0]['curdatetime'] < kits_data_recent[0]['dettime']:
        
        # create query for counts since most recent socrata data
        #  query start time must be in local US/Central time (KITSDB is naive!)
        strtime = arrow.get(socrata_data[0]['curdatetime']).to('US/Central').format('YYYY-MM-DD HH:mm:ss')

        #  INTID is KITS_ID in data tracker / socrata
        #  it uniquely identifies the radar device/location
        #  detname and the lane and should be queried from the DETECTORSRM
        #  table note that the values in the detname field in SYSDETHISTORYRM
        #  are not current and appear to be updated only the first time the
        #  detector is configured in KITS
        kits_query =  '''
            SELECT i.DETID as detid
            ,i.CURDATETIME as curdatetime
            ,i.VOLUME as volume
            ,i.SPEED as speed
            ,i.INTNAME as intname
            ,i.OCCUPANCY as occupancy
            ,e.INTID as int_id
            ,e.DETSN as detname
            FROM [KITS].[SYSDETHISTORYRM] i
            LEFT OUTER JOIN [KITS].[DETECTORSRM] e
            ON i.[DETID] = e.[DETID]
            WHERE (i.[CURDATETIME] >= '{}')
            ORDER BY CURDATETIME DESC
            '''.format(strtime)
    
    else:
        logger.info('No Data to export')
        return 0

    kits_data = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            kits_query
        )

    print('Processing date/time fields')

    for row in kits_data:
        row['month'] = row['curdatetime'].month
        row['day'] = row['curdatetime'].day
        row['year'] = row['curdatetime'].year
        row['day'] = row['curdatetime'].day
        row['hour'] = row['curdatetime'].hour
        row['minute'] = row['curdatetime'].minute
        row['day_of_week'] = row['curdatetime'].weekday()
        #  day of week is 0 to 6 starting on monday
        #  shit to 0 to 6 starting on sunday
        if row['day_of_week'] == 6:
            row['day_of_week'] = 0
        else:
            row['day_of_week'] = row['day_of_week'] + 1

        row['timebin'] = get_timebin(row['minute'], row['hour'])
        row['direction'] = get_direction( row['detname'].upper() )
    
    kits_data = datautil.replace_timezone(kits_data,'curdatetime')
    kits_data = datautil.iso_to_unix(kits_data,['curdatetime'])
    kits_data = datautil.stringify_key_values(kits_data)
    
    hash_fields = ['detid','curdatetime','detname']

    for row in kits_data:
        hasher = hashlib.md5()
        in_str = ''.join([str(row[q]) for q in hash_fields])
        hasher.update(in_str.encode('utf-8'))
        row['row_id'] = hasher.hexdigest()

    kits_data = datautil.stringify_key_values(kits_data)   

    socrata_payload = datautil.lower_case_keys(
        kits_data
    )

    status_upsert_response = socratautil.Soda(
        auth=SOCRATA_CREDENTIALS,
        records=socrata_payload,
        resource=socrata_resource,
        location_field=None
    )

    return (len(socrata_payload))

def cli_args():
    parser = argutil.get_parser(
        'count_data_pub.py',
        'Publish radar count data from KITS DB to City of Austin Open Data Portal.',
        '--replace'
    )
    
    args = parser.parse_args()
    
    return args


if __name__ == '__main__':

    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    
    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    socrata_resource = 'i626-g7ub'

    try:
        args = cli_args()
        replace = args.replace

        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)

        job.start()
        
        results = main()

        job.result('success', records_processed=results)

        logger.info('END AT {}'.format( arrow.now() ))

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(str(error_text))

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: Radar Traffic Count Publisher',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        job.result('error', message=str(e))

        raise e

