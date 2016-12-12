if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
import agol_helpers
import knack_helpers
import socrata_helpers
import email_helpers
import data_helpers
import secrets
import pandas
import pdb


#  KNACK CONFIG
PRIMARY_KEY = 'ATD_EVAL_ID'
 
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_53','object_56','object_12','object_21','object_14','object_13','object_26','object_27','object_29','object_36','object_63','object_31','object_35','object_37','object_41','object_42','object_43', 'object_45', 'object_58'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

BACKUP_DIRECTORY = secrets.BACKUP_DIRECTORY


now = arrow.now()

def main(date_time):

    try:       
        
        count = 0

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
            parsed = knack_helpers.ParseData(data, field_list, KNACK_PARAMS, convert_to_unix=True, include_ids=True)
            
            today = date_time.format('YYYY_MM_DD')
            
            file_name = '{}/{}_{}.csv'.format(BACKUP_DIRECTORY, obj, today)

            try:
                data_helpers.WriteToCSV(parsed, file_name=file_name)
            
            except Exception as e:
                print(e)
                continue

            count += 1

        return count



    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        body = 'Data backup of failed'            
        email_helpers.SendEmail(secrets['ALERTS_DISTRIBUTION'], 'data backup exception', body)
        raise e


r = main(now)

print( '{} objects written to file'.format(r) )


