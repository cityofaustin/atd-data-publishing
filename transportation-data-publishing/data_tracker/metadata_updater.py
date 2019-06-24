"""
Retrieve Knack app data and update new and changed object
and field metadata records.
"""
import json
import pdb

import arrow
import knackpy
import requests
import argutil
import datautil

import _setpath
from config.metadata.config import cfg
from config.secrets import *


def get_app_data(app_id):
    """Summary
    
    Args:
        app_id (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return knackpy.get_app_data(app_id)


def parse_fields(obj_data, conn_field, obj_row_id_lookup):
    """
    Extract field data from object data and append connection field
    https://www.knack.com/developer-documentation/#working-with-fields
    
    Args:
        obj_data (TYPE): Description
        conn_field (TYPE): Description
        obj_row_id_lookup (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    fields = []
    for obj in obj_data:
        object_id = obj["_id"]  #  this is the object's unique application ID
        object_row_id = obj_row_id_lookup[
            object_id
        ]  #  this is the row ID of the object in the admin metadata table

        for field in obj["fields"]:
            field[conn_field] = [object_row_id]
            fields.append(field)

    return fields


def get_filter(id_field_id, _id):
    """Summary
    
    Args:
        id_field_id (TYPE): Description
        _id (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return {
        "match": "and",
        "rules": [{"field": f"{id_field_id}", "operator": "is", "value": f"{_id}"}],
    }


def get_existing_data(obj_key, app_id, api_key):
    """Summary
    
    Args:
        obj_key (TYPE): Description
        app_id (TYPE): Description
        api_key (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    #  get admin_metadata data as Knack instance
    return knackpy.Knack(
        obj=obj_key, app_id=app_id, api_key=api_key, raw_connections=True
    )


def get_object_row_ids(object_metadata_records, id_field_key):
    """
    Return a dict where each key is an object's ID and
    each value is the corresponding row ID of that object in the admin
    metadata table. We need row ID so that we can populate the connection
    field between field metadata records and their object.
    
    Args:
        object_metadata_records (TYPE): Description
        id_field_key (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return {row[id_field_key]: row["id"] for row in object_metadata_records}


def evaluate_ids(new_data, old_data, id_field_key):
    """Summary
    
    Args:
        new_data (TYPE): Description
        old_data (TYPE): Description
        id_field_key (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    # each key in existing_ids is a record id in the admin table
    # each value is the application's record id of the field or object
    existing_ids = {record["id"]: record[id_field_key] for record in old_data}

    # each element in new_ids is the application's record id of the field or object
    new_ids = [record["_id"] for record in new_data]

    #  check for new fields/objects
    create = [
        record for record in new_data if record["_id"] not in existing_ids.values()
    ]

    #  identify already existing fields/objects
    update = []

    for row_id in existing_ids.keys():
        if existing_ids[row_id] in new_ids:
            for record in new_data:
                if record["_id"] == existing_ids[row_id]:
                    record["id"] = row_id
                    update.append(record)

                continue

    #  identify deleted fields/objects
    delete = []

    for _id in existing_ids.keys():
        if existing_ids[_id] not in new_ids:
            delete.append({"id": _id})

    return {"create": create, "update": update, "delete": delete}


def convert_bools_nones_arrays(dicts):
    """Summary
    
    Args:
        dicts (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    for d in dicts:
        for k in d:
            if d[k] == "No":
                d[k] = False
            elif d[k] == "Yes":
                d[k] = True
            elif d[k] == None:
                d[k] = "None"
            elif d[k] == []:
                d[k] = ""
    return dicts


def format_connections(dicts, conn_field):
    """
    Ugly method to extract record id from html string and format it as
    Knack connection field.
    
    We assume one connection per object in format:
        '<span class="5a6392415e5d9837a2ff7123">vision_zero_enforcement_shifts</´span>'
    
    or in format:
            ['5a6392415e5d9837a2ff7123']
    
        Returns record in format ['5a6392415e5d9837a2ff7123']
    
    Args:
        dicts (TYPE): Description
        conn_field (TYPE): Description
    
    Returns:
        TYPE: Description
    """
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


def update_records(payload, obj, method, app_name):
    """
    CRUD for Knack
    
    Args:
        payload (TYPE): Description
        obj (TYPE): Description
        method (TYPE): Description
        app_name (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    results = []

    for record in payload:

        res = knackpy.record(
            record,
            obj_key=obj,
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            method=method,
        )

        results.append(res)

    return results


def cli_args():
    parser = argutil.get_parser(
        "metadata_updater.py",
        "Retrieve Knack app data and update new and changed object and field metadata records.",
        "app_name",
    )

    args = parser.parse_args()

    return args


def main():
    args = cli_args()

    app_name = args.app_name

    records_processed = 0

    record_types = ["objects", "fields"]

    app_data = get_app_data(KNACK_CREDENTIALS[app_name]["app_id"])

    for record_type in record_types:

        data_new = app_data["objects"]

        if record_type == "fields":
            #  we get latest object data within this for loop
            #  because it may change when objects are processed
            data_existing_objects = get_existing_data(
                cfg["objects"]["obj"],
                KNACK_CREDENTIALS[app_name]["app_id"],
                KNACK_CREDENTIALS[app_name]["api_key"],
            )

            obj_row_id_lookup = get_object_row_ids(
                data_existing_objects.data_raw, cfg["objects"]["id_field_key"]
            )

            data_new = parse_fields(
                data_new, cfg[record_type]["object_connection_field"], obj_row_id_lookup
            )

        data_existing = get_existing_data(
            cfg[record_type]["obj"],
            KNACK_CREDENTIALS[app_name]["app_id"],
            KNACK_CREDENTIALS[app_name]["api_key"],
        )

        payload = evaluate_ids(
            data_new, data_existing.data_raw, cfg[record_type]["id_field_key"]
        )

        for method in payload.keys():

            if record_type == "fields":

                data_existing.data_raw = format_connections(
                    data_existing.data_raw, cfg[record_type]["object_connection_field"]
                )

            payload[method] = convert_bools_nones_arrays(payload[method])
            data_existing.data_raw = convert_bools_nones_arrays(data_existing.data_raw)

            payload[method] = datautil.stringify_key_values(
                payload[method], cfg[record_type]["stringify_keys"]
            )

            payload[method] = datautil.replace_keys(
                payload[method], data_existing.field_map
            )

            if method == "update":
                # verify if data has changed
                changed = []

                for rec_new in payload[method]:
                    rec_old = [
                        record
                        for record in data_existing.data_raw
                        if record["id"] == rec_new["id"]
                    ][0]

                    # format connection fields

                    # identify fields whose contents don't match
                    diff = [k for k in rec_new.keys() if rec_old[k] != rec_new[k]]

                    if diff:
                        changed.append(rec_new)
                    else:
                        continue

                payload[method] = changed

            update_records(payload[method], cfg[record_type]["obj"], method, app_name)

        records_processed += sum(
            [len(payload["create"]), len(payload["update"]), len(payload["delete"])]
        )

    return records_processed


if __name__ == "__main__":
    main()
