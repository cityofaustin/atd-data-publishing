# Get number of device online/offline/no communication and write to log table
# in Data Tracker.

# Attributes:
#     LOG_OBJ (str): Description

import argparse
from collections import defaultdict
import logging
import os
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *

from tdutils import argutil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import datautil
from tdutils import logutil


LOG_OBJ = "object_131"


def get_log_data(knack_creds):
    """Summary
    
    Args:
        knack_creds (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return knackpy.Knack(
        obj=LOG_OBJ,
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
        rows_per_page=1,
        page_limit=1,
    )


def build_payload(data, device_type):
    """Summary
    
    Args:
        data (TYPE): Description
        device_type (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    #  create a localized timestamp because Knack assumes timestamps are local
    now = arrow.now().replace(tzinfo="UTC").timestamp * 1000

    date_str = str(arrow.now().date())
    record_id = "{}-{}".format(device_type, date_str)

    return {
        "DEVICE_TYPE": data["DEVICE_TYPE"],
        "ONLINE": data["ONLINE"],
        "OFFLINE": data["OFFLINE"],
        "NO_COMMUNICATION": data["NO COMMUNICATION"],
        "STATUS_DATETIME": now,
        "RECORD_ID": record_id,
    }


def cli_args():

    parser = argutil.get_parser(
        "device_status_log.py",
        "Generate connectivity statistics and upload to Knack application.",
        "device_type",
        "app_name",
    )

    args = parser.parse_args()

    return args


def main():
    """Summary
    
    Args:
        job (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Description
    """

    args = cli_args()

    device_type = args.device_type
    app_name = args.app_name

    primary_key = cfg[device_type]["primary_key"]
    status_field = cfg[device_type]["status_field"]
    status_filters = cfg[device_type]["status_filter_comm_status"]

    knack_creds = KNACK_CREDENTIALS[app_name]

    kn = knackpy.Knack(
        obj=cfg[device_type]["obj"],
        scene=cfg[device_type]["scene"],
        view=cfg[device_type]["view"],
        ref_obj=cfg[device_type]["ref_obj"],
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
    )

    kn_log = get_log_data(knack_creds)

    stats = defaultdict(int)

    stats["DEVICE_TYPE"] = device_type

    for device in kn.data:
        #  count stats only for devices that are TURNED_ON
        if device[status_field] in status_filters:
            status = device["IP_COMM_STATUS"]
            stats[status] += 1

    payload = build_payload(stats, args.device_type)
    payload = datautil.replace_keys([payload], kn_log.field_map)

    res = knackpy.record(
        payload[0],
        obj_key=LOG_OBJ,
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
        method="create",
    )

    return len(payload)


if __name__ == "__main__":
    main()
