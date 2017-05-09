'''
check traffic signal prevent maintenance (PM) records and
insert copies of PM records to signals' secondary signals
'''
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import logging
import pdb
import arrow
import knack_helpers
import data_helpers
import email_helpers
import secrets

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

logfile = './log/sig_pm_copier_{}.log'.format(now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)

#  KNACK CONFIG
knack_creds = secrets.KNACK_CREDENTIALS

params_pm = {  
    'objects' : ['object_84', 'object_12'],
    'scene' : '416',
    'view' : '1182',
    'field_names' : ['ATD_PM_ID', 'SIGNAL', 'ATD_PM_ID', 'PM_COMPLETED_DATE', 'WORK_ORDER', 'PM_COMPLETED_BY', 'PM_STATUS', 'CONTROL', 'PRIMARY_SIGNAL_ID', 'COPIED_TO_SECONDARY', 'COPIED_FROM_PRIMARY', 'COPIED_FROM_ID']
}

params_signal = {  
    'objects' : ['object_12'],
    'scene' : '73',
    'view' : '197',
    'field_names' : ['SIGNAL_ID','CONTROL', 'PRIMARY_SIGNAL', 'SECONDARY_SIGNALS']
}

copy_fields = ['PM_COMPLETED_DATE', 'WORK_ORDER', 'PM_COMPLETED_BY']





def get_prim_signals(list_of_signals):
    '''
    create a dict of primary signals with and the secondary signals they control
    expects list_of_signals to have 'KNACK_ID', i.e knack data has been parsed
    with option include_ids=True
    '''
    signals_with_children = {}

    for signal in list_of_signals:
        if 'SECONDARY_SIGNALS' in signal:
            if len(signal['SECONDARY_SIGNALS']) > 0:
                signals_with_children[signal['KNACK_ID']] = signal['SECONDARY_SIGNALS']
                    
    return signals_with_children



def copy_pm_record(destination_signal_id, source_record, copy_fields):
    
    new_record = {
        'SIGNAL' : [destination_signal_id],
        'COPIED_FROM_ID' : source_record['ATD_PM_ID'],
        'PM_STATUS': 'COMPLETED',
        'COPIED_FROM_PRIMARY' : True,
        'COPIED_TO_SECONDARY' : False
    }

    for field in copy_fields:
        if field in source_record:
            new_record[field] = source_record[field]

    return new_record



def main(date_time):
    print('starting stuff now')

    try:       
        field_dict = knack_helpers.get_fields(params_pm['objects'], knack_creds)
       
        field_lookup = knack_helpers.create_field_lookup(field_dict, parse_raw=True)
        knack_data_pm = knack_helpers.get_data(params_pm['scene'], params_pm['view'], knack_creds)        
        knack_data_pm = knack_helpers.parse_data(knack_data_pm, field_dict, require_locations=False, raw_connections=True, convert_to_unix=False, include_ids=True)
               
        field_dict = knack_helpers.get_fields(params_signal['objects'], knack_creds)
        knack_data_signals = knack_helpers.get_data(params_signal['scene'], params_signal['view'], knack_creds)
        knack_data_signals = knack_helpers.parse_data(knack_data_signals, field_dict, require_locations=False, convert_to_unix=False, include_ids=True, raw_connections=True)
        primary_signals_with_children = get_prim_signals(knack_data_signals)

        payload_insert = []
        payload_update = []

        for pm in knack_data_pm:
            '''
            check all preventative maintenance records at signals with secondary signals
            copy pm record to secondary signal if needed
            '''
            if 'SIGNAL' in pm:
                if pm['COPIED_TO_SECONDARY'] == False and pm['PM_STATUS'] == 'COMPLETED':

                    primary_signal_id = pm['SIGNAL'][0]['id']

                    if primary_signal_id in primary_signals_with_children:
                        #  update original pm record with copied to secondary = True
                        payload_update.append({ 'KNACK_ID' : pm['KNACK_ID'], 'COPIED_TO_SECONDARY' : True })

                        for secondary in primary_signals_with_children[primary_signal_id]:
                            #  create new pm record for secondary signal(s)
                            new_record = copy_pm_record(secondary['id'], pm, copy_fields)
                            payload_insert.append(new_record)

        payload_update = data_helpers.replace_keys(payload_update, field_lookup)
        payload_insert = data_helpers.replace_keys(payload_insert, field_lookup)
                        
        count = 0

        update_response = []
        
        if len(payload_insert) == 0:
            logging.info('No PM records to copy.')
            return "No PM records to copy."
    
        for record in payload_update:
            count += 1
            print( 'update record {} of {}'.format( count, len(payload_insert) ) )
            logging.info('update record {} of {}'.format( count, len(payload_insert) ) )
            response_json = knack_helpers.update_record(record, params_pm['objects'][0], 'KNACK_ID', knack_creds)
            logging.info(response_json)
            update_response.append(response_json)

        count = 0

        for record in payload_insert:
            count += 1
            print( 'insert record {} of {}'.format( count, len(payload_insert) ) )
            logging.info('insert record {} of {}'.format( count, len(payload_insert) ) )
            response_json = knack_helpers.insert_record(record, params_pm['objects'][0], 'KNACK_ID', knack_creds)
            logging.info(response_json)
            update_response.append(response_json)

        return "done"
        

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Copy Preventative Maintenance Failure', str(e))
        logging.error( str(e) )

        print(e)
        raise e


results = main(now)

print(results)



