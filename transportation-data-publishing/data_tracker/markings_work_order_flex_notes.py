"""
Connect markings work orders to the flex notes that are connected to their connected
311 Service Request.
"""

from pprint import pprint as print
import pdb

import argutil
import knackpy
import knackutil

import _setpath
from config.knack.config import MARKINGS_WORK_ORDERS_FLEX_NOTES as cfg
from config.secrets import *


def filter_flex_note(field, value):
    return [{"field": f"{field}", "operator": "contains", "value": f"{value}"}]


def knackpy_wrapper(cfg, auth, filters=None):

    return knackpy.Knack(
        scene=cfg["scene"],
        view=cfg["view"],
        obj=cfg["obj"],
        ref_obj=cfg["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
    )


def main():

    args = cli_args()

    auth = KNACK_CREDENTIALS[args.app_name]

    # gets work orders have a connected CSR, but no flex notes
    # this filtering is done in the knack view
    work_orders = knackpy_wrapper(cfg["work_orders"], auth)

    if not work_orders.data:
        return 0

    for wo in work_orders.data:
        payload = []

        sr_number = wo.get(cfg["work_orders"]["sr_number_field_name"])

        wo_id = wo.get("id")

        # fetch all flex notes connected to each service request
        # the Knack view is filtered to exclude the "XFERIOSK" flex note
        # which is just "allow record to be published to external interfaces"
        _filter = filter_flex_note(cfg["flex_notes"]["sr_number_field_id"], sr_number)

        flex_notes = knackpy_wrapper(cfg["flex_notes"], auth, filters=_filter)

        for fn in flex_notes.data_raw:
            fn[cfg["flex_notes"]["work_order_connection_field_id"]] = [wo_id]

            # reduce flex note to record id, work order connection field
            fn_payload = {}

            for key in fn.keys():
                if key in cfg["flex_notes"]["payload_field_ids"]:
                    fn_payload[key] = fn[key]

            payload.append(fn_payload)

        for record in payload:

            res = knackpy.record(
                record,
                obj_key=cfg["flex_notes"]["ref_obj"][0],
                app_id=auth["app_id"],
                api_key=auth["api_key"],
                method="update",
            )

    return len(work_orders.data)


def cli_args():
    parser = argutil.get_parser(
        "markings_awork_order_flex_notes.py",
        "Connect markings work orders to their related flex notes.",
        "app_name",
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
