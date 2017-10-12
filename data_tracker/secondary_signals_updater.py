'''
Update traffic signal records with secondary signal relationships
'''
import argparse
import collections
import logging
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import datautil
from util import emailutil


def cli_args():
    parser = argparse.ArgumentParser(
        prog='secondary_signals_updater.py',
        description='Update traffic signal records with secondary signal relationships'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        help='Name of the knack application that will be accessed'
    )

    args = parser.parse_args()    
    return(args)


def get_new_prim_signals(signals):
    '''
    create a dict of primary signals and the secondary signals they control
    data is compiled from the 'primary_signal' field on secondary signals
    this field is maintained by ATD staff via the signals forms in the knack database
    '''
    signals_with_children = {}

    for signal in signals:

        try:
            #  get id of parent signal
            knack_id = signal['PRIMARY_SIGNAL'][0]['id']  
        except (IndexError, AttributeError):
            #  empty key
            continue

        if knack_id not in signals_with_children:
            #  add entry for parent signal
            signals_with_children[knack_id] = []  
        #  add current signal to list of parent's children
        signals_with_children[knack_id].append(signal['id'])  

    return signals_with_children



def get_old_prim_signals(signals):
    '''
    create a dict of primary signals and the secondary signals they control
    data is compiled from the 'secondary_signals' field on primary signals
    this field is populated by this Python service
    '''
    signals_with_children = {}
    
    for signal in signals:
        knack_id = signal['id']

        secondary_signals = []
            
        try:    
            for secondary in signal['SECONDARY_SIGNALS']:
                
                secondary_signals.append(secondary['id'])

                signals_with_children[knack_id] = secondary_signals

        except (KeyError, AttributeError):
            continue
    
    return signals_with_children            



def main(date_time):
    print('starting stuff now')

    try:       
        kn = knackpy.Knack(
            scene=scene,
            view=view,
            ref_obj=ref_obj,
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            raw_connections=True
        )

        primary_signals_old = get_old_prim_signals(kn.data)
        primary_signals_new = get_new_prim_signals(kn.data)
        
        payload = []
        
        for signal_id in primary_signals_new:
            '''
            identify new and changed primary-secondary relationships
            '''
            if signal_id in primary_signals_old:
                new_secondaries = collections.Counter(primary_signals_new[signal_id])
                old_secondaries = collections.Counter(primary_signals_old[signal_id])
                
                if old_secondaries != new_secondaries:
                    
                    payload.append({
                        'id' : signal_id,
                        update_field : primary_signals_new[signal_id]
                    })

            else:
                 payload.append({
                    'id' : signal_id,
                    update_field : primary_signals_new[signal_id]
                })

        for signal_id in primary_signals_old:
            '''
            identify primary-secondary relationships that have been removed
            '''
            if signal_id not in primary_signals_new:
                payload.append({ 'id' : signal_id, update_field : [] })

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
        
            response_json = knackpy.update_record(
                record, ref_obj[0], 
                'id', knack_creds['app_id'],
                knack_creds['api_key']
            )

            update_response.append(response_json)

        logging.info( "Record updates sent :{}".format(len(update_response)) )
        logging.info( "Response: {}".format(update_response) )
        return update_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        error_text = traceback.format_exc()
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'Update Secondary Signals Failure',
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )

        print(e)
        raise e

if __name__ == '__main__':
    #  parse command-line arguments
    args = cli_args()
    app_name = args.app_name

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    logfile = '{}/{}_{}.log'.format(LOG_DIRECTORY, 'update_sec_signals', now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    update_field = 'field_1329'  # SECONDARY_SIGNALS field
    ref_obj = ['object_12']  #  Signals object
    scene = 'scene_73'
    view = 'view_197'

    knack_creds = KNACK_CREDENTIALS[app_name]

    results = main(now)
    logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)



