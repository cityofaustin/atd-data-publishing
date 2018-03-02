import os
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import emailutil
from util import datautil
from util import logutil

now = arrow.now()
script = os.path.basename(__file__).replace('.py', '.log')

logfile = f'{LOG_DIRECTORY}/{script}'
logger = logutil.timed_rotating_log(logfile)
logger.info('START AT {}'.format(str(now)))

objects = ['object_87', 'object_93', 'object_77', 'object_53', 'object_96', 'object_83', 'object_95', 'object_21', 'object_14', 'object_109', 'object_73', 'object_110', 'object_15', 'object_36', 'object_11', 'object_107', 'object_115', 'object_116', 'object_117', 'object_67', 'object_91', 'object_89', 'object_12', 'object_118', 'object_113', 'object_98', 'object_102', 'object_71', 'object_84', 'object_13', 'object_26', 'object_27', 'object_81', 'object_82', 'object_7', 'object_42', 'object_43', 'object_45', 'object_75', 'object_58', 'object_56', 'object_54', 'object_86', 'object_78', 'object_85', 'object_104', 'object_106', 'object_31', 'object_101', 'object_74', 'object_94', 'object_9', 'object_10', 'object_19', 'object_20', 'object_24', 'object_57', 'object_59', 'object_65', 'object_68', 'object_76', 'object_97', 'object_108']
app_name = 'data_tracker_prod'
backup_directory = BACKUP_DIRECTORY
knack_credentials = KNACK_CREDENTIALS[app_name]

field_names = []


def main(date_time):

    try:       
        
        count = 0

        for obj in objects:
            logger.info( "backup {}".format(obj) )

            kn = knackpy.Knack(
                obj=obj,
                app_id=KNACK_CREDENTIALS[app_name]['app_id'],
                api_key=KNACK_CREDENTIALS[app_name]['api_key']
            )

            if kn.data:
                logger.info( "total records: {}".format(len(kn.data)) )

                today = date_time.format('YYYY_MM_DD')
                
                file_name = '{}/{}_{}.csv'.format(backup_directory, obj, today)
                date_fields_kn = [kn.fields[f]['label'] for f in kn.fields if kn.fields[f]['type'] in ['date_time', 'date']]

                kn.data = datautil.mills_to_iso(kn.data, date_fields_kn)
                
                try:
                    kn.to_csv(file_name)
                
                except UnicodeError:
                    kn.data = [{key : str(d[key]).encode()} for d in kn.data for key in d ]
                    kn.to_csv(file_name)

                count += 1
            
            else:
                continue

        return count

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        body = 'Data backup of failed'
        emailutil.send_email(ALERTS_DISTRIBUTION, 'Data Bakup Exception', str(e), EMAIL['user'], EMAIL['password'])
        raise e


r = main(now)

print( '{} objects written to file'.format(r) )


