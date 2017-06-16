if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import logging
import pdb
import arrow

import knack_helpers
import email_helpers
import data_helpers
import secrets


now = arrow.now()
now_s = now.format('YYYY_MM_DD')

log_directory = secrets.LOG_DIRECTORY
logfile = '{}/{}_{}.log'.format(log_directory,'backup_objs', now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

objects = ['object_11', 'object_53','object_56','object_12','object_21','object_14','object_13','object_26','object_27','object_29','object_36','object_63','object_31', 'object_70', 'object_35','object_37','object_41','object_42','object_43', 'object_45', 'object_58', 'object_82', 'object_81', 'object_78', 'object_84', 'object_85', 'object_89', 'object_91']

backup_directory = secrets.BACKUP_DIRECTORY
knack_credentials = secrets.KNACK_CREDENTIALS
log_directory = secrets.LOG_DIRECTORY

field_names = []


def main(date_time):

    try:       
        
        count = 0

        for obj in objects:
            logging.info( "backup {}".format(obj) )

            #  get field metadata
            fields = knack_helpers.get_all_fields(obj, knack_credentials)
            
            #  assign field metadata to 'raw' field name
            field_list = {}
            for field in fields:
                field_list[field['key'] + '_raw'] = field
            
            #  get knack object data
            data = knack_helpers.get_object_data(obj, knack_credentials)
            logging.info( "total records: {}".format(len(data)) )

            #  parse data
            parsed = knack_helpers.parse_data(data, field_list, convert_to_unix=True, include_ids=True)
            
            today = date_time.format('YYYY_MM_DD')
            
            file_name = '{}/{}_{}.csv'.format(backup_directory, obj, today)

            try:
                data_helpers.write_csv(parsed, file_name=file_name)
            
            except Exception as e:
                print(e)
                body = 'Data backup of failed when writing csv'            
                email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'data backup exception', body)
                raise e

            count += 1

        return count



    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        body = 'Data backup of failed'            
        email_helpers.send_email(secrets['ALERTS_DISTRIBUTION'], 'data backup exception', body)
        raise e


r = main(now)

print( '{} objects written to file'.format(r) )


