"""
Connect markings work orders  to the flex notes that are connected to their connected
311 Service Request.
"""

# todo: created a test CSR: 19-00280974. But we're waiting for the ISSUE_TYPE to popualte through to the flext notes, i think.

from pprint import pprint as print
import pdb

import argutil
import knackpy
import knackutil

import _setpath
from config.knack.config import MARKINGS_WORK_ORDERS_FLEX_NOTES as cfg
from config.secrets import *


def knackpy_wrapper(cfg_dataset, auth):
    return knackpy.Knack(
        scene=cfg_dataset["scene"],
        view=cfg_dataset["view"],
        ref_obj=cfg_dataset["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        rows_per_page=10,  # TODO: remove record limits
        page_limit=1,  # TODO: remove record limits
    )


def main():

    args = cli_args()

    auth = KNACK_CREDENTIALS[args.app_name]

    kn_work_orders = knackpy_wrapper(cfg["work_orders"], auth)

    pdb.set_trace()

    
    """
    TODO:
    - extract knack work order id
    - get each flex note record id for SR number of connected work order
    - update flex note work order connection field with work order knack record id
    """
    if not kn.data:
        return 0

    return len(kn.data)


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
