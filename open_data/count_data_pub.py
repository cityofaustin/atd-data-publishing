import logging
import pdb
import sys

import arrow
import knackpy

import hashlib
import argparse

import _setpath
from config.secrets import *
from util import kitsutil
from util import datautil
from util import emailutil
from util import socratautil   


# Issues:
# Can't import more than the 5,000 socrata limit to get the most recent entryk



then = arrow.now()
now_s = then.format('YYYY_MM_DD')

logfile = '{}/dms_msg_pub_{}.log'.format(LOG_DIRECTORY, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(then)))

# #  config
# primay_key = 'DMS_ID'
# knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
# ref_obj = ['object_109']
# scene = 'scene_569'
# view = 'view_1564'

socrata_counts = 'i626-g7ub'


    
def main(date_time):
    print('starting stuff now')

    try:  

        #finds most recent KITS data
        kits_query_recent =   '''
            SELECT TOP (1) DETID as det_id
            ,CURDATETIME as dettime
            ,DETNAME as lane
            ,VOLUME as vol
            ,SPEED as spd
            FROM [KITS].[SYSDETHISTORYRM]
            ORDER BY CURDATETIME DESC
            '''   


        recent_kits = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            kits_query_recent
        )


        socr = socratautil.Soda(
            socrata_counts,
            user=SOCRATA_CREDENTIALS['user'],
            password=SOCRATA_CREDENTIALS['password'],
            soql = {'$order':'curdatetime desc','$limit':1}
        )

        # most recent socrata data
        # https://data.austintexas.gov/resource/i626-g7ub.json?$order=curdatetime desc&$limit=1
        #https://data.austintexas.gov/resource/i626-g7ub.json?$where=curdatetime>1509516000
        for record in recent_kits:
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



        # if the socrata data is behind KITS data send new data
        elif soc_data[0]['curdatetime'] < recent_kits[0]['dettime']:
            
            pdb.set_trace()
            #creates query for counts since most recent socrata data
            strtime = str(soc_data[0]['year'])
            strtime+=('-')
            if int(soc_data[0]['month']) <= 9:
                strtime+=('0')
            strtime+= str(soc_data[0]['month'])
            strtime+=('-')
            if int(soc_data[0]['day']) <= 9:
                strtime+=('0')
            strtime += str(soc_data[0]['day'])
            strtime+=' '
            if int(soc_data[0]['hour']) <= 9:
                strtime+=('0')
            strtime += str(soc_data[0]['hour'])
            strtime+=(':')
            if int(soc_data[0]['minute']) <= 9:
                strtime+=('0')
            strtime += str(soc_data[0]['minute'])
            strtime+=(':00.000')


            kits_query =  '''
                SELECT DETID as detid
                ,CURDATETIME as curdatetime
                ,DETNAME as detname
                ,VOLUME as volume
                ,SPEED as speed
                ,INTNAME as intname
                ,OCCUPANCY as occupancy
                FROM [KITS].[SYSDETHISTORYRM]
                WHERE (CURDATETIME >= '{}')
                ORDER BY CURDATETIME DESC
                '''.format(strtime)
            
            
        else:
            print('No Data to export, try: count_data_pub.py -replace')
            return True


        kits_data = kitsutil.data_as_dict(
                KITS_CREDENTIALS,
                kits_query
            )
        

        int_query =   '''
            SELECT DETID as detid
            ,INTID as int_id
            ,DETSN as detname
            FROM [KITS].[DETECTORSRM]
            '''
        int_data = kitsutil.data_as_dict(
            KITS_CREDENTIALS,
            int_query
        )

        pdb.set_trace()
        bins = [15, 30, 45]
        for row in kits_data:
            row['month'] = row['curdatetime'].month
            row['day'] = row['curdatetime'].day
            row['year'] = row['curdatetime'].year
            row['day'] = row['curdatetime'].day
            row['hour'] = row['curdatetime'].hour
            row['minute'] = row['curdatetime'].minute
            row['day_of_week'] = row['curdatetime'].weekday()
            if row['day_of_week'] == 6:
                row['day_of_week'] == 0
            else:
                row['day_of_week'] = row['day_of_week'] + 1

            if row['hour'] == 0:
                row['timebin'] = '00'
                if row['minute'] in bins:
                    row['timebin'] += ':' + str(row['minute'])
                elif row['minute'] > 52 or row['minute'] <= 7:
                    row['timebin'] += ':' + '00'
                elif row['minute'] > 7 or row['minute'] <= 22:
                    row['timebin'] += ':' + str(15)
                elif row['minute'] > 22 or row['minute'] <= 37:
                    row['timebin'] += ':' + str(30)
                elif row['minute'] > 37 or row['minute'] <= 52:
                    row['timebin'] += ':' + str(45)
                else:
                    row['timebin'] = ''

            elif len(str(row['hour'])) == 1:
                row['timebin'] = '0'
                if row['minute'] in bins:
                    row['timebin'] += str(row['hour']) + ':' + str(row['minute'])
                elif row['minute'] in bins:
                    row['timebin'] += str(row['hour']) + ':' + str(row['minute'])
                elif row['minute'] > 52 or row['minute'] <= 7:
                    row['timebin'] += str(row['hour']) + ':' + '00'
                elif row['minute'] > 7 or row['minute'] <= 22:
                    row['timebin'] += str(row['hour']) + ':' + str(15)
                elif row['minute'] > 22 or row['minute'] <= 37:
                    row['timebin'] += str(row['hour']) + ':' + str(30)
                elif row['minute'] > 37 or row['minute'] <= 52:
                    row['timebin'] += str(row['hour']) + ':' + str(45)
                else:
                    row['timebin'] = ''

            else:
                if row['minute'] in bins:
                    row['timebin'] = str(row['hour']) + ':' + str(row['minute'])
                elif row['minute'] in bins:
                    row['timebin'] = str(row['hour']) + ':' + str(row['minute'])
                elif row['minute'] > 52 or row['minute'] <= 7:
                    row['timebin'] = str(row['hour']) + ':' + '00'
                elif row['minute'] > 7 or row['minute'] <= 22:
                    row['timebin'] = str(row['hour']) + ':' + str(15)
                elif row['minute'] > 22 or row['minute'] <= 37:
                    row['timebin'] = str(row['hour']) + ':' + str(30)
                elif row['minute'] > 37 or row['minute'] <= 52:
                    row['timebin'] = str(row['hour']) + ':' + str(45)
                else:
                    row['timebin'] = ''




        

        pdb.set_trace()

        kits_data = datautil.replaceTimezone(kits_data,'curdatetime')
        kits_data = datautil.iso_to_unix(kits_data,['curdatetime'])

        kits_data = datautil.stringify_key_values(kits_data)
        
        #hash ID + Date + Lane
        pdb.set_trace()
        
        hash_fields = ['detid','curdatetime','detname']
        for row in kits_data:
            hasher = hashlib.md5()
            in_str = ''.join([str(row[q]) for q in hash_fields])
            hasher.update(in_str.encode('utf-8'))
            row['row_id'] = hasher.hexdigest()

        pdb.set_trace()

        for line in kits_data:
            for detect in int_data:
                if detect['detid'] == int(line['detid']):
                    line['int_id'] = detect['int_id']
                    line['detname'] = detect['detname']

        kits_data = datautil.stringify_key_values(kits_data)
        pdb.set_trace()

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


        pdb.set_trace()

        kits_data = datautil.upper_case_keys(kits_data)

        pdb.set_trace()

        socrata_payload = datautil.lower_case_keys(
            kits_data
        )
        

        pdb.set_trace()
        status_upsert_response = socratautil.upsert_data(
            SOCRATA_CREDENTIALS,
            socrata_payload,
            socrata_counts
        )

        pdb.set_trace()
        
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        # emailutil.send_email(
        #     ALERTS_DISTRIBUTION,
        #     'DATA PROCESSING ALERT: DMS Message Update',
        #     str(e),
        #     EMAIL['user'],
        #     EMAIL['password']
        # )

        raise e


 
def cli_args():
    parser = argparse.ArgumentParser(
        prog='count_data_pub.py',
        description='Publishes radar count data from KITS DB to Soccrata (open data portal).'
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
