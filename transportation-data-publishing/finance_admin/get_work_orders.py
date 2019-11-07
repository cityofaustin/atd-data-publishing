"""
Get AMD work orders from the Data Tracker and upsert them
to the Finance & Purchasing System so that they can be incorporated
w/ inventory requests.
"""
from pprint import pprint as print
import pdb

import argutil
import datautil
import knackpy
import knackutil

import _setpath
from config.secrets import KNACK_CREDENTIALS
from config.knack.config import cfg


FINANCE_ADMIN_CONFIG = {
    "data_tracker_finance_record_id_field_name": "FINANCE_ADMIN_RECORD_ID",  # column in data tracker work orders that contains the record id of the work order in the finance/admin system. if it exists.
    "data_tracker_finance_record_id_field_id": "field_3424",  # data tracker work orders that contains the record id of the work order in the finance/admin system. if it exists
    "work_orders_object": "object_33",  # This is the work orders objet in the finance & purchasing system
    "finance_data_tracker_record_id_field_id": "field_758",  # field ID in Finance & Purchasing system that contains the Knack record ID of the matching record in the Data Tracker
}

FIELDMAP = {
    "CREATED_DATE": "field_757",
    "LOCATION_NAME": "field_722",
    "WORK_NEEDED": "field_723",
    "TECHNICIAN_LEAD": "field_724",
    "TECHNICIAN_SUPPORT": "field_725",
    "SIGNAL_ID": "field_726",
    "SCHOOL_ZONE": "field_727",
    "ATD_WORK_ORDER_ID": "field_721",
    "WORK_ORDER_STATUS": "field_728",
    "id": "field_758",  # the Data Tracker record ID stored in the Finance & Purchasing system in this field
    "ZONE_NAME": "field_727",
    FINANCE_ADMIN_CONFIG["data_tracker_finance_record_id_field_name"]: "id",
}


def build_data_tracker_payload(
    finance_admin_record_id, data_tracker_id, data_tracker_finance_record_id_field_id
):

    # prepare a payload to update the data tracker work order with the finance_admin record id
    return {
        "id": data_tracker_id,
        data_tracker_finance_record_id_field_id: finance_admin_record_id,
    }


def post_record(record, auth, obj_key, method):

    res = knackpy.record(
        record,
        obj_key=obj_key,
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        method=method,
    )

    return res


def knackpy_wrapper(cfg_dataset, auth, filters=None):
    return knackpy.Knack(
        obj=cfg_dataset["obj"],
        scene=cfg_dataset["scene"],
        view=cfg_dataset["view"],
        ref_obj=cfg_dataset["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        page_limit=3,
        rows_per_page=1000,
    )


def map_fields(list_of_dicts, fieldmap):
    mapped = []

    for record in list_of_dicts:
        new_record = {}

        for key in record.keys():
            if key in FIELDMAP.keys():
                new_record[FIELDMAP[key]] = record[key]

        mapped.append(new_record)

    return mapped


def cli_args():

    parser = argutil.get_parser(
        "get_work_orders.py",
        "Get AMD work orders and upload to Finance & Purchasing system",
        "--replace",
        "--last_run_date",
    )

    parsed = parser.parse_args()

    return parsed


def main():
    config_data_tracker = cfg["work_orders_signals"]

    args = cli_args()

    auth_data_tracker = KNACK_CREDENTIALS["data_tracker_prod"]

    auth_finance_admin = KNACK_CREDENTIALS["finance_admin_prod"]

    last_run_date = args.last_run_date

    if not last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = "1970-01-01"

    filters = knackutil.date_filter_on_or_after(
        last_run_date, config_data_tracker["modified_date_field_id"]
    )

    # get work order data from Data Tracker
    work_orders = knackpy_wrapper(
        config_data_tracker, auth_data_tracker, filters=filters
    )

    if not work_orders:
        return 0

    # prepare payload for finace & purchasing system
    payload = map_fields(work_orders.data, FIELDMAP)

    records_processed = 0

    for record in payload:

        data_tracker_record_id = record.get(FIELDMAP["id"])

        if record.get("id"):
            method = "update"

        else:
            method = "create"

        new_record = post_record(
            record,
            auth_finance_admin,
            FINANCE_ADMIN_CONFIG["work_orders_object"],
            method,
        )

        if method == "create":
            # update data tracker with record id of matching record in finance system, if it doens't already exist
            data_tracker_update = build_data_tracker_payload(
                new_record["id"],
                data_tracker_record_id,
                FINANCE_ADMIN_CONFIG["data_tracker_finance_record_id_field_id"],
            )

            res = post_record(
                data_tracker_update,
                auth_data_tracker,
                config_data_tracker["ref_obj"][0],
                "update",
            )

        records_processed += 1
        print(records_processed)

    return records_processed


if __name__ == "__main__":
    main()
