'''
update traffic signal records with secondary relationships
'''

if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import collections
import logging
import pdb
import arrow
import knack_helpers
import data_helpers
import email_helpers
import secrets

log_directory = secrets.LOG_DIRECTORY

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

logfile = '{}/{}_{}.log'.format(log_directory, 'update_sec_signals', now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

update_field = 'field_1329'  # SECONDARY_SIGNALS field
objects = ['object_12']
scene = '73'
view = '197'

knack_creds = secrets.KNACK_CREDENTIALS


def get_new_prim_signals(list_of_signals):
    '''
    create a dict of primary signals and the secondary signals they control
    data is compiled from the 'primary_signal' field on secondary signals
    this field is maintained by ATD staff via the signals forms in the knack database
    '''
    signals_with_children = {}

    for signal in list_of_signals:

        if 'PRIMARY_SIGNAL' in signal:
            
            primary_knack_id = signal['PRIMARY_SIGNAL'][0]['id']

            if primary_knack_id not in signals_with_children:
                signals_with_children[primary_knack_id] = []

            signals_with_children[primary_knack_id].append(signal['KNACK_ID'])

    return signals_with_children



def get_old_prim_signals(list_of_signals):
    '''
    create a dict of primary signals and the secondary signals they control
    data is compiled from the 'secondary_signals' field on priamry signals
    this field is populated by this Python service
    '''
    signals_with_children = {}

    for signal in list_of_signals:
        primary_knack_id = signal['KNACK_ID']

        if 'SECONDARY_SIGNALS' in signal:
            secondary_signals = []
            
            for secondary in signal['SECONDARY_SIGNALS']:
                secondary_signals.append(secondary['id'])

            signals_with_children[primary_knack_id] = secondary_signals
    
    return signals_with_children            



def main(date_time):
    print('starting stuff now')

    try:       


        field_dict = knack_helpers.get_fields(objects, knack_creds)
        
        knack_data = knack_helpers.get_data(scene, view, knack_creds)

        knack_data = knack_helpers.parse_data(knack_data, field_dict, require_locations=False, convert_to_unix=True, include_ids=True, raw_connections=True)
        
        primary_signals_old = get_old_prim_signals(knack_data)
        
        primary_signals_new = get_new_prim_signals(knack_data)
        
        payload = []
        
        for signal_id in primary_signals_new:
            '''
            identify new and changed primary-secondary relationships
            '''
            if signal_id in primary_signals_old:
                
                new_secondaries = collections.Counter(primary_signals_new[signal_id])
                old_secondaries = collections.Counter(primary_signals_old[signal_id])
    
                if old_secondaries != new_secondaries:
                    payload.append({ 'KNACK_ID' : signal_id, update_field : primary_signals_new[signal_id] })

            else:
                 payload.append({ 'KNACK_ID' : signal_id, update_field : primary_signals_new[signal_id] })

        for signal_id in primary_signals_old:
            '''
            identify primary-secondary relationships that have been removed
            '''
            if signal_id not in primary_signals_new:
                payload.append({ 'KNACK_ID' : signal_id, update_field : [] })

        count = 0
        update_response = []
        
        if len(payload) == 0:
            logging.info("No new secondary signals.")
            return "No new secondary signals."

        logging.info( "{} records to update".format(len(payload)) )
        logging.info( "{} records to update".format(payload) )
        
        for record in payload:
            count += 1
            print( 'updating record {} of {}'.format( count, len(payload) ) )
        
            response_json = knack_helpers.update_record(record, objects[0], 'KNACK_ID', knack_creds)

            update_response.append(response_json)

        logging.info( "Record updates sent :{}".format(len(update_response)) )
        logging.info( "Response: {}".format(update_response) )
        return update_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Update Secondary Signals Failure', str(e))
        print(e)
        raise e

results = main(now)
logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)



