'''
Extract radar traffic count data from KITS database and publish
new records to City of Austin Open Data Portal.
'''
import argparse
import hashlib
import logging
import pdb
import sys

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import datautil
from util import emailutil
from util import kitsutil
from util import socratautil   

then = arrow.now()
now_s = then.format('YYYY_MM_DD')
logfile = '{}/radar_counts_{}.log'.format(LOG_DIRECTORY, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(then)))

socrata_resource = 'i626-g7ub'

def myRound(x, base=15):
    # https://stackoverflow.com/questions/2272149/round-to-5-or-other-number-in-python
    return int(base * round(float(x)/base))


def getTimebin(minute, hour):
    '''
    Round an arbitrary minue/hour to the nearest 15 minutes
    '''
    minute = myRound(minute)
    hour_offset = 0
    
    if minute == 60:
        minute = 0
        hour_offset = 1

    timebin = '{}:{}'.format(hour + hour_offset, minute)

    return arrow.get(timebin,'H:m').format('HH:mm')


def getDirection(lane):
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


def main(date_time):
    try:  
        #  find most recent KITS data
        kits_query_recent =   '''
            SELECT TOP (1) DETID as det_id
            ,CURDATETIME as dettime
            ,DETNAME as lane
            ,VOLUME as vol
            ,SPEED as spd
            FROM [KITS].[SYSDETHISTORYRM]
            ORDER BY CURDATETIME DESC
            '''   

        #  get most recent traffic count record from kits
        kits_data_recent = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            kits_query_recent
        )

        #  get most recent traffic count record from socrata
        socr = socratautil.Soda(
            socrata_resource,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password'],
            soql = {
                '$order':'curdatetime desc',
                '$limit':1
            }
        )

        #  
        for record in kits_data_recent:
            new_date = arrow.get(record['dettime'],'US/Central')
            record['dettime'] = new_date.timestamp

        soc_data = socr.data
        
        if replace:
            # get all kits data
            kits_query =   '''
                SELECT DETID as detid
                ,CURDATETIME as curdatetime
                ,DETNAME as detname
                ,VOLUME as volume
                ,SPEED as speed
                ,INTNAME as intname
                ,OCCUPANCY as occupancy
                FROM [KITS].[SYSDETHISTORYRM]
                ORDER BY CURDATETIME DESC
                '''
        
        # send new data if the socrata data is behind KITS data
        elif soc_data[0]['curdatetime'] < kits_data_recent[0]['dettime']:
            
            # create query for counts since most recent socrata data
            #  query start time must be in local US/Central time (KITSDB is naive!)
            strtime = arrow.get(soc_data[0]['curdatetime']).to('local').format('YYYY-MM-DD HH:mm:ss')

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
            logging.info('No Data to export, try: count_data_pub.py -replace')
            return True

        kits_data = kitsutil.data_as_dict(
                KITS_CREDENTIALS,
                kits_query
            )

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

            row['timebin'] = getTimebin(row['minute'], row['hour'])
            row['direction'] = getDirection(row['detname'])
        
        kits_data = datautil.replaceTimezone(kits_data,'curdatetime')
        kits_data = datautil.iso_to_unix(kits_data,['curdatetime'])
        kits_data = datautil.stringify_key_values(kits_data)
        
        hash_fields = ['detid','curdatetime','detname']

        for row in kits_data:
            hasher = hashlib.md5()
            in_str = ''.join([str(row[q]) for q in hash_fields])
            hasher.update(in_str.encode('utf-8'))
            row['row_id'] = hasher.hexdigest()

        kits_data = datautil.stringify_key_values(kits_data)

        socr.get_metadata()
        fieldnames = socr.fieldnames
        socr_data = datautil.reduce_to_keys(socr.data, fieldnames)
        date_fields = socr.date_fields
        socr_data = datautil.upper_case_keys(socr_data)
        socr_data = datautil.stringify_key_values(socr_data)

        for record in socr_data:
            rec_keys = list(record.keys())
            if 'ROW_ID' not in rec_keys:
                hasher = hashlib.sha1()
                hash_fields = ['DETID','CURDATETIME','DETNAME']
                in_str = ''.join([str(record[q]) for q in hash_fields])
                hasher.update(in_str.encode('utf-8'))
                record['ROW_ID'] = hasher.hexdigest()

        kits_data = datautil.upper_case_keys(kits_data)

        socrata_payload = datautil.lower_case_keys(
            kits_data
        )

        status_upsert_response = socratautil.upsert_data(
            SOCRATA_CREDENTIALS,
            socrata_payload,
            socrata_resource
        )

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: Radar Traffic Count Publisher',
            str(e),
            EMAIL['user'],
            EMAIL['password']
        )

        raise e

def cli_args():
    parser = argparse.ArgumentParser(
        prog='count_data_pub.py',
        description='Publish radar count data from KITS DB to City of Austin Open Data Portal.'
    )

    parser.add_argument(
            '-replace',
            action='store_true',
            default=False,
            help='Ignores date restrictions on updating data and replaces all data.'
    )

    args = parser.parse_args()
    
    return(args)

if __name__ == '__main__':
    #  parse command-line arguments    
    args = cli_args()
    replace = args.replace
    results = main(then)

logging.info('Elapsed time: {}'.format(str(arrow.now() - then)))
