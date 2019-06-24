"""
fulcrum / knack integration enginge


todo:
    - left off mapping fields for insert
    - you want to design the field map in a way that in can be passed with a 'from/to' param
    - and handle either 'direction'
    - assigned to id lookup/mapping
    - logging
"""

import argparse
import pdb

from fulcrum import Fulcrum
import knackpy

import _setpath
from config.config import *
from config.secrets import *
import fulcutil


def cli_args():
    """Summary
    
    Returns:
        TYPE: Description
    """
    parser = argparse.ArgumentParser(
        prog="fulcrum/knack data sync",
        description="Synchronize data between Knack application and Fulcrum application",
    )

    parser.add_argument(
        "knack",
        type=str,
        choices=["data_tracker_prod", "data_tracker_test_fulcrum"],
        help="Name of the Knack application that we be accessed.",
    )

    parser.add_argument(
        "fulcrum",
        type=str,
        choices=["work_orders_prod"],
        help="Name of the fulcrum app that will be accessed.",
    )

    args = parser.parse_args()

    return args


def get_records_knack(app_name, config, endpoint_type="private"):
    """Summary
    
    Args:
        app_name (TYPE): Description
        config (TYPE): Description
        endpoint_type (str, optional): Description
    
    Returns:
        TYPE: Description
    """
    api_key = KNACK_CREDENTIALS[app_knack]["api_key"]
    app_id = KNACK_CREDENTIALS[app_knack]["app_id"]

    if endpoint_type == "public":
        return knackpy.Knack(scene=config["scene"], view=config["view"], app_id=app_id)

    else:
        return knackpy.Knack(
            scene=config["scene"],
            view=config["view"],
            ref_obj=config["ref_obj"],
            api_key=api_key,
            app_id=app_id,
        )


def map_fields(record, field_map, *, method):
    """Summary
    
    Args:
        record (TYPE): Description
        field_map (TYPE): Description
        method (TYPE): Description
    """
    pdb.set_trace()
    print("ok")


# KNACK_FULC_FIELDMAP = [
#     {
#         'name_knack' : 'id',
#         'name_fulcrum' : 'knack_id',
#         'detect_changes' : False,
#         'table_fulcrum' : 'form',
#         'type_fulcrum' : str,
#         'type_knack' : str,
#     }
# ]


def get_field_data(fulc, form_id):
    """Summary
    
    Args:
        fulc (TYPE): Description
        form_id (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    #  move to fulcutil
    form = fulc.forms.find(form_id)
    return form["form"][0]["elements"]


def get_fulcrum_id(knack_record, api_key, table):
    """
    Lookup record in Fulcrum and append record ID to input record dict
    
    Args:
        knack_record (TYPE): Description
        api_key (TYPE): Description
        table (TYPE): Description
    
    Returns:
        TYPE: Description
    
    Raises:
        Exception: Description
    """
    query = fulcutil.get_query_by_value("knack_id", knack_record["id"], table)
    res = fulcutil.query(api_key, query)

    if len(res["rows"]) == 1:
        knack_record["fulcrum_id"] = res["rows"][0]["_record_id"]
        return knack_record

    elif len(res["rows"]) == 0:
        knack_record["fulcrum_id"] = None
        return knack_record

    else:
        raise Exception("Multiple records found with same ID!")


def update_fulcrum(*, record, task, api_key, form_id):
    """Summary
    
    Args:
        record (TYPE): Description
        task (TYPE): Description
        api_key (TYPE): Description
        form_id (TYPE): Description
    
    Returns:
        TYPE: Description
    
    Raises:
        Exception: Description
    """
    record = map_fields(record, KNACK_FULC_FIELDMAP, method="knack_to_fulcrum")
    payload = fulcutil.get_template()
    payload = fulcutil.format_record(record, payload, form_id)
    fulcrum = Fulcrum(key=api_key)

    if task == "create":
        res = fulcrum.records.create(payload)

    elif task == "update":
        res = fulcrum.records.update(form_id, payload)

    else:
        raise Exception(f"Uknown update task: {task}")

    return res


def main(app_knack, app_fulcrum):
    """Summary
    
    Args:
        app_knack (TYPE): Description
        app_fulcrum (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    #  get configuration for Knack and Fulcrum
    api_key_fulcrum = FULCRUM[app_fulcrum]["api_key"]
    form_id_fulcrum = FULCRUM[app_fulcrum]["form_id"]
    cfg_knack_public = CFG_KNACK_FULCRUM[app_knack]["work_orders_public"]
    cfg_knack_private = CFG_KNACK_FULCRUM[app_knack]["work_orders_private"]

    #  move this somewhere else
    results_fulcrum = {"update": 0, "insert": 0, "errors": 0}
    results_knack = {"update": 0, "insert": 0, "errors": 0}

    #  check public knack endpoint for records
    kn_public = get_records_knack(app_knack, cfg_knack_public, endpoint_type="public")

    if not kn_public.data:
        #  no knack records to update in fulcrum
        return None

    #  get fulcrum user records
    users_fulcrum = fulcutil.get_users(api_key_fulcrum, form_id_fulcrum)

    #  get complete records data from private endpoint
    kn = get_records_knack(app_knack, cfg_knack_private, endpoint_type="private")

    for record in kn.data:

        record = get_fulcrum_id(record, api_key_fulcrum, "Work Orders")

        if record["fulcrum_id"]:
            continue

        else:
            task = "create"
            res = update_fulcrum(
                record=record,
                task=task,
                api_key=api_key_fulcrum,
                form_id=form_id_fulcrum,
            )

        print(task)
        pdb.set_trace()

        if res.status_code == 200:
            res = update_knack(kn_record)
            results[task] += 1

        else:
            print("SHIT!")
            results[task] += 1


if __name__ == "__main__":

    args = cli_args()
    app_knack = args.knack
    app_fulcrum = args.fulcrum

    main(app_knack, app_fulcrum)
