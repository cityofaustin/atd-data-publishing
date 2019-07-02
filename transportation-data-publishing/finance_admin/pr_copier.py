'''
Copy purchase requests.

#TODO: update tdutils with new app name...sigh
'''
import pdb
import os
import pdb

import arrow
import knackpy

import _setpath
from config.knack.config import PURCHASE_REQUEST_COPIER as cfg
from config.secrets import *

import datautil

def knackpy_wrapper(cfg, auth, filters=None, raw_connections=False):

    return knackpy.Knack(
        obj=cfg.get("obj"),
        scene=cfg.get("scene"),
        view=cfg.get("view"),
        ref_obj=cfg.get("ref_obj"),
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        raw_connections=raw_connections
    )


def handle_fields(record, fields, field_map):
    '''
    Parse connection and equation fields
    '''
    handled = {}

    for key in record.keys():
        field_id = field_map.get(key)

        # re-format connection field for upload
        if fields.get(field_id).get('type') == 'connection':
            handled[key] = [ item['id'] for item in record[key] ]

        # drop field types that should not be manually created
        elif fields.get(field_id).get('type') in ['equation', 'concatenation', 'auto_increment']:
            continue

        else:
            handled[key] = record[key]

    return handled


def get_filter(field_id, val):
    return {"field": f"{field_id}", "operator": "is", "value": f"{val}"},


def get_items(cfg, filters, auth):
    return knackpy_wrapper(cfg, auth, filters=filters, raw_connections=True)


def assign_requester(cfg, record):
    '''
    Set the "Requester" vield to the value in the "COPIED_BY" field.
    '''
    record[cfg['requester_field_id']] = record.pop(cfg['copied_by_field_id'])
    return record


def main():

    app_name = "finance_admin_prod" #TODO: add to argutil

    if "finance" not in app_name:
        raise Exception('Unsupported application specified. Must be finance_admin_prod or finance_admin_test.')
    
    knack_creds = KNACK_CREDENTIALS[app_name]

    '''
    We start by making a "free" call to the API endpoint to check for records.
    This calls an endpoint that is not behind login, and we do not provide a
    reference object, which avoivds making a call for field data.

    This way we do not accure API usage when checking for records to process.
    '''
    free_creds = {
        'app_id' : knack_creds['app_id'],
        'api_key' : None
    }

    free_cfg = dict(cfg["purchase_requests"])

    free_cfg.pop('ref_obj')

    free_prs = knackpy_wrapper(
        free_cfg,
        knack_creds, 
        raw_connections=True,
    )
    
    if not free_prs.data_raw:
        return 0

    '''
    There is data to be processed, so make a standard request for the record
    and field data.
    '''
    prs = knackpy_wrapper(
        cfg["purchase_requests"],
        knack_creds, 
        raw_connections=True,
    )

    for record in prs.data:
        # this grabs the aut increment field value, which is then droppped
        pr_filter_id = record.get(cfg["purchase_requests"]['unique_id_field_name'])

        old_record_id = record.pop('id')

        record = handle_fields(record, prs.fields, prs.field_map)

        record = datautil.replace_keys(
            [record],
            prs.field_map
        )[0]

        record = assign_requester(cfg["purchase_requests"], record)

        #  Set the "copy" field to No
        record[cfg["purchase_requests"]['copy_field_id']] = False

        copied_record = knackpy.record(
            record,
            obj_key=cfg["purchase_requests"]['ref_obj'][0],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            method='create'
        )

        #  update the older record with need_to_copy=false
        old_record_payload = {
            'id' : old_record_id,
            cfg["purchase_requests"]['copy_field_id'] : False
        }

        old_record_update = knackpy.record(
            old_record_payload,
            obj_key=cfg["purchase_requests"]['ref_obj'][0],
            app_id=knack_creds['app_id'],
            api_key=knack_creds['api_key'],
            method='update'
        )

        # fetch item records related to the copied purchase request, and copy
        # them to the new purchase request
        item_filter = get_filter(cfg["items"]['pr_field_id'], pr_filter_id)
        items = get_items(cfg["items"], item_filter, knack_creds)

        for item in items.data:
            item = handle_fields(item, items.fields, items.field_map)
            
            # set item connection to copied purchase request record
            item[cfg["items"]['pr_connection_field_name']] = [copied_record['id']]

            item.pop('id')
            
            item = datautil.replace_keys(
                [item],
                items.field_map
            )[0]

            new_item = knackpy.record(
                item,
                obj_key=cfg["items"]['obj'],
                app_id=knack_creds['app_id'],
                api_key=knack_creds['api_key'],
                method='create'
            )

    return len(records)        


if __name__ == '__main__':
    results = main()
