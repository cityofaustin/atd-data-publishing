'''
Retrieve Knack app data and update new and changed object
and field metadata records.
'''
import argparse
import json
import logging
import os
import pdb
import traceback

import arrow
import knackpy
import requests

import _setpath
from config.metadata.config import cfg
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

record_types = ['objects', 'fields']

def get_app_data(app_id):
    return knackpy.get_app_data(app_id)


def parse_fields(obj_data, conn_field, obj_row_id_lookup):
    '''
    Extract field data from object data and append connection field
    https://www.knack.com/developer-documentation/#working-with-fields
    '''
    fields = []
    for obj in obj_data:
        object_id = obj['_id']  #  this is the object's unique application ID
        object_row_id = obj_row_id_lookup[object_id] #  this is the row ID of the object in the admin metadata table    
        
        for field in obj['fields']:
            field[conn_field] = [object_row_id]
            fields.append(field)

    return fields


def get_filter(id_field_id, _id):
    return {
        'match': 'and',
            'rules': [
                {
                    'field':f'{id_field_id}',
                    'operator':'is',
                    'value':f'{_id}'
                }
            ]
        }
    

def get_existing_data(obj_key, app_id, api_key):
    #  get admin_metadata data as Knack instance
    return knackpy.Knack(
        obj=obj_key,
        app_id=app_id,
        api_key=api_key,
        raw_connections=True
    )


def get_object_row_ids(object_metadata_records, id_field_key):
    '''
    Return a dict where each key is an object's ID and
    each value is the corresponding row ID of that object in the admin
    metadata table. We need row ID so that we can populate the connection
    field between field metadata records and their object.
    '''
    return { row[id_field_key] : row['id']  for row in object_metadata_records }


def evaluate_ids(new_data, old_data, id_field_key):

    # each key in existing_ids is a record id in the admin table
    # each value is the application's record id of the field or object
    existing_ids = { record['id'] : record[id_field_key] for record in old_data }
    
    # each element in new_ids is the application's record id of the field or object
    new_ids = [record['_id'] for record in new_data]
    
    #  check for new fields/objects
    create = [record for record in new_data if record['_id'] not in existing_ids.values()]
    
    #  identify already existing fields/objects
    update = []
    
    for row_id in existing_ids.keys():
        if existing_ids[row_id] in new_ids:
            for record in new_data: 
                if record['_id'] == existing_ids[row_id]:
                    record['id'] = row_id
                    update.append(record)

                continue

    #  identify deleted fields/objects
    delete = []
    
    for _id in existing_ids.keys():
        if existing_ids[_id] not in new_ids:
            delete.append({ 'id' : _id })
    
    return {
        'create' : create,
        'update' : update,
        'delete' : delete,
    }
        

def convert_bools_nones_arrays(dicts):
    for d in dicts:
        for k in d:
            if d[k] == 'No':
                d[k] = False
            elif d[k] == 'Yes':
                d[k] = True
            elif d[k] == None:
                d[k] = 'None'
            elif d[k] == []:
                d[k] = '' 
    return dicts
 

def format_connections(dicts, conn_field):
    '''
    Ugly method to extract record id from html string and format it as
    Knack connection field.
    
    We assume one connection per object in format:
        '<span class="5a6392415e5d9837a2ff7123">vision_zero_enforcement_shifts</´span>'
    or in format:
        '5a6392415e5d9837a2ff7123'
    or in format:
        ['5a6392415e5d9837a2ff7123']

    Returns record in format ['5a6392415e5d9837a2ff7123']
    '''
    count = 0
    for record in dicts:
        if record.get(conn_field):
            try:
                #  hand as an html connection string
                _id = record[conn_field].split('"')[1]
                record[conn_field] = _id
            except IndexError:
                #  handle as a record_id string
                record[conn_field] = [record[conn_field]]
            except AttributeError:
                #  hand as record id array
                pass
        else:
            record = []

    return dicts


def update_records(payload, obj, method):
    '''
    CRUD for Knack
    '''
    results = []

    for record in payload:
        
        res = knackpy.record(
            record,
            obj_key=obj,
            app_id=KNACK_CREDENTIALS[app_name]['app_id'],
            api_key=KNACK_CREDENTIALS[app_name]['api_key'],
            method=method
        )

        results.append(res)

    return results


def cli_args():
    parser = argutil.get_parser(
        'metadata_updater.py',
        'Retrieve Knack app data and update new and changed object and field metadata records.',
        'app_name',
    )
    
    args = parser.parse_args()
    
    return args

def main(job):

    results = []
    app_data = get_app_data(KNACK_CREDENTIALS[app_name]['app_id'])

    for record_type in record_types:

        data_new = app_data['objects']

        if record_type == 'fields':
            #  we get latest object data within this for loop
            #  because it may change when objects are processed
            data_existing_objects = get_existing_data(
                cfg['objects']['obj'],
                KNACK_CREDENTIALS[app_name]['app_id'],
                KNACK_CREDENTIALS[app_name]['api_key'],
            )

            obj_row_id_lookup = get_object_row_ids(
                data_existing_objects.data_raw,
                cfg['objects']['id_field_key']
            )

            data_new = parse_fields(
                data_new,
                cfg[record_type]['object_connection_field'],
                obj_row_id_lookup
            )

        data_existing = get_existing_data(
            cfg[record_type]['obj'],
            KNACK_CREDENTIALS[app_name]['app_id'],
            KNACK_CREDENTIALS[app_name]['api_key'],
        )

        payload = evaluate_ids(
            data_new,
            data_existing.data_raw,
            cfg[record_type]['id_field_key']
        )

        for method in payload.keys():

            if record_type == 'fields':
                data_existing.data_raw = format_connections(
                    data_existing.data_raw,
                    cfg[record_type]['object_connection_field']
                )

            payload[method] = convert_bools_nones_arrays(payload[method])
            data_existing.data_raw = convert_bools_nones_arrays(
                data_existing.data_raw)

            payload[method] = datautil.stringify_key_values(payload[method],
                                                            cfg[record_type][
                                                                'stringify_keys'])

            payload[method] = datautil.replace_keys(
                payload[method],
                data_existing.field_map
            )

            if method == 'update':
                # verify if data has changed
                changed = []

                for rec_new in payload[method]:
                    rec_old = [record for record in data_existing.data_raw if
                               record['id'] == rec_new['id']][0]

                    # format connection fields

                    # identify fields whose contents don't match
                    diff = [k for k in rec_new.keys() if
                            rec_old[k] != rec_new[k]]

                    if diff:
                        changed.append(rec_new)
                    else:
                        continue

                payload[method] = changed

            logger.info(
                len('{} {} record'.format(method, len(payload[method]))))

            update_records(
                payload[method],
                cfg[record_type]['obj'],
                method
            )

        message = '{}: create: {}; update: {}; delete: {}'.format(
            record_type,
            len(payload['create']),
            len(payload['update']),
            len(payload['delete'])
        )

        results.append(message)

    return results



if __name__ == '__main__':
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'

    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    args = cli_args()
    app_name = args.app_name

    try:
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)
        
        job.start()

        results = main(job)

        logger.info('END AT {}'.format( arrow.now() ))

        job.result('success', message=' | '.join(results) )

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(error_text)
        
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            "Metadata Update Failure",
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )

        job.result('error')

        raise e




