'''
Assign detection status to traffic signal based on status of its detectors. 
Update detection status log when signal detection status changes.
'''
import os
import logging
import traceback
from collections import defaultdict
import pdb
import arrow
import data_helpers
import knack_api as kn
import secrets

def groupBySignal(detector_data):
    '''
    Group signal detector status and status date according to parent signal. 

     Parameters
    ----------
    detector_data : list | (required)
        List of dicts where each dict contains vehicle detector data
        retrieved from Knack view. attributes include signal id, detector
        status, and detector status date. 

    Returns
    -------
    det_status : dict
        Each key in dict is a traffic signal id with keys statuses (an array of detector
        statuses) and dates (an array of detector status dates). This dict will be used to
        determine most current status and date. see methond getStatus().
    '''
    det_status = defaultdict(dict)

    for det in  detector_data:
        if 'SIGNAL_ID' in det and DET_STATUS_LABEL in det and DET_DATE_LABEL in det:
            sig = '${}'.format(det['SIGNAL_ID'])  #  format signal ID as string 
            status = det[DET_STATUS_LABEL]
            status_date = det[DET_DATE_LABEL]
            
            if sig not in det_status:
                det_status[sig]['statuses'] = [status]
                det_status[sig]['dates'] = [status_date]
            else:
                det_status[sig]['statuses'].append(status)
                det_status[sig]['dates'].append(status_date)

    return det_status


def getStatus(sig, det_status):
    '''
    Determine a signal's detection status based on the status
    of its detectors

    Parameters
    ----------
    sig : dict | (required)
        A signal record dict generated from a Knack.View instance
    det_status : dict | (required)
        A lookup dictionary generated from method groupBySignal()

    Returns
    -------
    value : string
        A detection status string of BROKEN, UNKNOWN, NO DETECTIO, OK
    '''
    sig_id = '${}'.format(sig['SIGNAL_ID'])

    if sig_id in det_status:
        #  any broken detector, status is BROKEN
        if 'BROKEN' in det_status[sig_id]['statuses']:
            return 'BROKEN'
        #  any unknown detector, status is UNKNOWN
        if 'UNKNOWN' in det_status[sig_id]['statuses']:
            return 'UNKNOWN'
        #  detection has been removed and not updated, or who knows what
        if 'OK' not in det_status[sig_id]['statuses']:
            return 'UNKNOWN'
        #  detection must be OK
        return 'OK'
    else:
        #  no detectors at signal
        return  'NO DETECTION'

def getMaxDate(sig, det_status):
    '''
    Determine a signal's most recent status date status 

    Parameters
    ----------
    sig : dict | (required)
        A signal record dict generated from a Knack.View instance
    det_status : dict | (required)
        A lookup dictionary generated from method groupBySignal()

    Returns
    -------
    value : int
        A timestamp of the maximum (ie most recent) detection status date
    '''
    sig_id = '${}'.format(sig['SIGNAL_ID'])

    if sig_id in det_status:
        return max([int(t) for t in det_status[sig_id]['dates'] ])
    else:
        return arrow.now().format('MM-DD-YYYY')

if __name__ == '__main__':

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    #  init logging 
    log_directory = secrets.LOG_DIRECTORY
    cur_dir = os.path.dirname(__file__)
    logfile = '{}/detection_status_{}.log'.format(log_directory, now_s)
    log_path = os.path.join(cur_dir, logfile)
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))

    try:
        #  field labels on detector object that provides source status and date
        DET_STATUS_LABEL = 'DETECTOR_STATUS'
        DET_DATE_LABEL = 'MODIFIED_DATE'
        #  field labels on signals object that will receive status and date
        SIG_STATUS_LABEL = 'DETECTION_STATUS'
        SIG_DATE_LABEL = 'DETECTION_STATUS_DATE'

        #  Knack API config
        api_key = secrets.KNACK_CREDENTIALS['api_key']
        app_id = secrets.KNACK_CREDENTIALS['app_id']

        config_detectors = {
            'scene' : 'scene_468',
            'view' : 'view_1333',
            'objects' : ['object_98']
        }

        config_signals = {
            'scene' : 'scene_73',
            'view' : 'view_197',
            'objects' : ['object_12']
        }

        config_status_log = {
            'objects' : ['object_102']
        }

        fieldmap_status_log = {
            "EVENT" : 'field_1576',
            "SIGNAL" : 'field_1577',
            "EVENT_DATE" : 'field_1578'
        }

        #  get detector data
        detectors = kn.View(scene=config_detectors['scene'], view=config_detectors['view'], field_obj=config_detectors['objects'], api_key=api_key, app_id=app_id)

        #  get signal data
        signals = kn.View(scene=config_signals['scene'], view=config_signals['view'], field_obj=config_signals['objects'], api_key=api_key, app_id=app_id)
        signals.data_parsed = data_helpers.filter_by_key(signals.data_parsed, 'SIGNAL_STATUS', ['TURNED_ON'])

        #  staging dict
        lookup = groupBySignal(detectors.data_parsed)

        #  record update count
        count_sig = 0
        count_status = 0

        #  iterate through signals, get status, update record in Knack database
        for sig in signals.data_parsed:
            
            old_status = None
            new_status = getStatus(sig, lookup)
            new_status_date = getMaxDate(sig, lookup)
            
            if SIG_STATUS_LABEL in sig:
                old_status = sig[SIG_STATUS_LABEL]

                if old_status == new_status:
                    #  no change in status
                    continue

            payload_signals = {
                'KNACK_ID' : sig['KNACK_ID'],
                SIG_STATUS_LABEL : new_status,
                SIG_DATE_LABEL : getMaxDate(sig, lookup)
            }

            #  replace field labels with database fieldnames
            payload_signals = data_helpers.replace_keys([payload_signals], signals.field_map)

            #  update signal record with detection status and date
            res = kn.update_record(payload_signals[0], config_signals['objects'][0], 'KNACK_ID', app_id, api_key)
        
            count_sig += 1
    
            #  update signal status log
            if not old_status and new_status == 'OK':  # detection is new
                event = 'DETECTION INSTALLED'
            elif new_status == 'OK' and old_status != 'OK':  #  detection restored
                event = 'DETECTION RESTORED'
            elif new_status == 'NO DETECTION':  #  detection uninstalled
                event = 'DETECTION UNINSTALLED'
            else:
                event = 'ISSUE REPORTED'  #  detection not ok

            payload_status_log = {
                'EVENT' : event,
                'EVENT_DATE' : new_status_date,
                'SIGNAL' : [sig['KNACK_ID']],  #  signal connection field is passed as an array of length 1
            }
            
            #  replace field labels with database fieldnames
            payload_status_log = data_helpers.replace_keys([payload_status_log], fieldmap_status_log)
            
            #  update signal detection status log
            res = kn.insert_record(payload_status_log[0], config_status_log['objects'][0], app_id, api_key)
            
            count_status += 1

        logging.info('{} signal records updated'.format(count_sig))
        logging.info( '{} detection status log records updated'.format(count_status) )
        logging.info('END AT {}'.format(str( arrow.now().format()) ))

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        error_text = traceback.format_exc()
        email_subject = "Detection Status Update Failure".format(dataset)
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, email_subject, error_text)
        logging.error(error_text)
        print(e)
        raise e

