"""
Connect markings work orders  to the flex notes that are connected to their connected
311 Service Request.
"""

import pdb

import argutil
import knackpy
import knackutil

import _setpath
from config.knack.config import cfg
from config.secrets import *


def knackpy_wrapper(cfg_dataset, auth):
    return knackpy.Knack(
        scene=cfg_dataset["scene"],
        view=cfg_dataset["view"],
        ref_obj=cfg_dataset["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
    )


def main():

    args = cli_args()
    
    auth = KNACK_CREDENTIALS[args.app_name]

    cfg = cfg["MARKINGS_WORK_ORDERS_FLEX_NOTES"]

    kn = knackpy_wrapper(cfg_dataset, auth)

    pdb.set_trace()
    
    if not kn.data:
        return 0

    return len(kn.data)


def cli_args():
    parser = argutil.get_parser(
        "markings_awork_order_flex_notes.py",
        "Connect markings work orders to their related flex notes.",
        "app_name"
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
