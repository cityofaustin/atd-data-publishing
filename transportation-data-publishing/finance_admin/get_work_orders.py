"""
Get AMD work orders from the Data Tracker and upsert them
to the Finance & Purchasing System so that they can be incorporated
w/ inventory requests.

TODO
- todo: enforce unique work_order_id in finance and admin to prevent dupes
- get all modified work orders
- convert connection objects, to short text if needed
- upsert them to f&p
"""
from pprint import pprint as print

import argutil
import datautil
import knackpy
import knackutil

import _setpath
from config.secrets import KNACK_CREDENTIALS
from config.knack.config import cfg

FINANCE_ADMIN_CONFIG = {
    "data_tracker_finance_record_id_field" : "FINANCE_ADMIN_RECORD_ID" # column in data tracker work orders that contains the record id of the work order in the finance/admin system. if it exists.
}

FIELDMAP = {}


def post_records(payload, config):
    records_processed = 0

    for record in payload:
        if payload.get(FINANCE_ADMIN_CONFIG["data_tracker_finance_record_id_field"]):
            # record already exists in finance system
            method = "update"
            
        else:
            method = "create"

        res = upload_to_finance(record)
        payload = build_data_tracker_payload(record["id"])
        res = update_data_tracker(payload)
        records_processed +=1
    
    return records_processed


def knackpy_wrapper(cfg_dataset, auth, filters=None):
    return knackpy.Knack(
        obj=cfg_dataset["obj"],
        scene=cfg_dataset["scene"],
        view=cfg_dataset["view"],
        ref_obj=cfg_dataset["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        page_limit=1, #TODO repalce this with 1000
        rows_per_page=10, #tODO remove this param
    )


def map_fields(list_of_dicts, fieldmap):
    mapped = []

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

    # config_finance_admin = cfg["config_finance_admin"]
    
    args = cli_args()

    auth_data_tracker = KNACK_CREDENTIALS["data_tracker_prod"]

    auth_finance_admin= KNACK_CREDENTIALS["finance_admin_prod"]

    last_run_date = args.last_run_date

    if not last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = "1970-01-01"

    filters = knackutil.date_filter_on_or_after(
        last_run_date,config_data_tracker["modified_date_field_id"]
    )

    # TODO: filter by status?

    work_orders = knackpy_wrapper(config_data_tracker, auth_data_tracker, filters=filters)

    if not work_orders:
        return 0

    import pdb; pdb.set_trace()

    payload = map_fields(work_orders.data_raw, FIELDMAP)

    records_processed = post_records(payload, auth_finance_admin)

    return records_processed

if __name__=="__main__":
    main()
    






