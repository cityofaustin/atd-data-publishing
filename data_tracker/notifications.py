'''
Send email notifications

TODO
- get filters working
- configs
- format messages

'''
import argparse
import logging
import pdb

import arrow
import knackpy

import _setpath
from config.cfg_ntfn import cfg
from config.secrets import *
from util import datautil
from util import emailutil

def cli_args():
    parser = argparse.ArgumentParser(
        prog='notifications.py',
        description='Send email notifications from Knack application'
    )

    parser.add_argument(
        'ntfn_name',
        action="store",
        type=str,
        help='The name of the notifcation to be processed'
    )

    parser.add_argument(
        'app_name',
        action="store",
        type=str,
        help='Name of the knack application that will be accessed'
    )

    args = parser.parse_args()    
    return(args)


def recip_filters(ntfn_name):
    
    return {
        #  see Knack API docs for filter formatting
        'match' : 'and',
        'rules' : [
            {
               'field' : 'FILTERING_FIELDWITH_NOTIFICAITONNAME_SELCTIONS',
               'operator' : 'contains << CHECK THIS!',
               'value' : ntfn_name
            }
        ]
    }



def get_recipients(creds, filters):
    '''
    Get contact addresses of notification recipients
    '''
    kn = knackpy.Knack(
        obj=cfg['recipients']['obj'],
        scene=cfg['recipients']['scene'],
        view=cfg['recipients']['view'],
        ref_obj=cfg['recipients']['ref_obj'],
        app_id=creds[app_name]['app_id'],
        api_key=creds[app_name]['api_key'],
        filter=filters
    )

    return kn.data


def record_filters(ntfn_name):
    
    return {
        #  see Knack API docs for filter formatting
        'match' : 'and',
        'rules' : [
            {
               'field' : cfg[ntfn_name]['filter_field'],
               'operator' : 'is << CHECK THIS!',
               'value' : False << CHECK THIS TOO OBVI
            }
        ]
    }


def get_records(creds, filters):

    kn = knackpy.Knack(
        obj=cfg[dataset]['obj'],
        scene=cfg[dataset]['scene'],
        view=cfg[dataset]['view'],
        ref_obj=cfg[dataset]['ref_obj'],
        app_id=creds['app_id'],
        api_key=creds['api_key'],
        filters=filters
    )

    return kn


def build_ntfn():
    print('build the notification message')


def send_ntfn():
    print('send the nofitication')


def update_records():
    print('turn off the sendnotificaiton flag on updated recorts')


def main(app_name, ntfn_name, creds):

    filters_recip = recip_filters(ntfn_name)
    recip = get_recipients(creds, filters)

    filters_rec = record_filters(ntfn_name)
    records = get_records(creds, filters_rec)
    
    for rec in record:
        msg = build_ntft(rec)
        send_ntfn(recip, msg['subject'], msg['body'])
        update_record(rec)


if __name__ == '__main__':
    args = cli_args()
    app_name = args.app_name
    ntfn_name = args.ntfn_name

    logfile = '{}/notification_{}_{}.log'.format(
        LOG_DIRECTORY,
        ntfn_name,
        arrow.now().format('YYYY_MM_DD')
    )

    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format( arrow.now().format() ))

    knack_creds = KNACK_CREDENTIALS[app_name]
    results = main(now)

    logging.info('END AT {}'.format( arrow.now().format() )
    print(results)    
















