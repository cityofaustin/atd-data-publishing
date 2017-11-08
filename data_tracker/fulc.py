#  fulcrum / knack integration enginge
# create environment
# get knack records that need to be published
#   check if fulcrum record exists and update it
#   otherwise create new

import argparse
import pdb

from fulcrum import Fulcrum
import knackpy

import _setpath
from config.config_fulcrum import *
from config.secrets import *


def cli_args():
    parser = argparse.ArgumentParser(
        prog='fulcrum/knack data sync',
        description='Synchronize data between Knack application and Fulcrum application'
    )

    parser.add_argument(
        'knack',
        type=str,
        choices=['data_tracker_prod', 'data_tracker_test_fulcrum'],
        help="Name of the Knack application that we be accessed."
    )

    parser.add_argument(
        'fulcrum',
        type=str,
        choices=['work_orders_prod'],
        help='Name of the fulcrum app that will be accessed.'
    )

    args = parser.parse_args()
    
    return(args)


def get_records_knack(app_name):
    api_key_knack = KNACK_CREDENTIALS[app_knack]['api_key']
    app_id_knack = KNACK_CREDENTIALS[app_knack]['app_id']
    knackpy.Knack()


def get_records_fulcrum(app_name):
    api_key_fulcrum = FULCRUM[app_fulcrum]['api_key']
    form_id_fulcrum = FULCRUM[app_fulcrum]['form_id']

    fulcrum = Fulcrum(api_key_fulcrum)
    form = fulcrum.forms.find(form_id_fulcrum)
    fields = forms['forms'][0]['elements']

    pdb.set_trace()
    recs = fulcrum.records.search(url_params={'form_id' : app_id_fulcrum })
    
    pdb.set_trace()
    

    return recs['records'], form

    # rec = {
    #     'record': {
    #         'form_values': {
    #             'work_type': 'Knockdown'
    #         },
    #         'form_id' : FULCRUM['form_id']
    #     }
    # }


if __name__ == '__main__':
    args = cli_args()
    app_knack = args.knack
    app_fulcrum = args.fulcrum

    data, form = get_updates_fulcrum(app_fulcrum)
    pdb.set_trace()
    data_kn = get_records_knack(app_knack)
    get_records_fulcrum(app_fulcrum, data_kn)

