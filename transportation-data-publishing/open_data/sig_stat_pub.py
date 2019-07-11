# Attributes:
#     FLASH_STATUSES (list): Description
#     SOCR_SIG_RES_ID (str): Description
#     SOCR_SIG_STAT_RES_ID (str): Description

import os
import pdb
import sys

import arrow
import kitsutil
import datautil
import emailutil
import jobutil
import logutil
import socratautil

import _setpath
from config.knack.config import cfg
from config.secrets import *

# define config variables

SOCR_SIG_RES_ID = "xwqn-2f78"
SOCR_SIG_STAT_RES_ID = "5zpr-dehc"
FLASH_STATUSES = ["1", "2", "3"]


def add_ids(records, primary_key="signal_id", id_field="record_id"):
    """
    Generate a unique record ID which is a concatenation of the signal ID and the current time
    
    Args:
        records (TYPE): Description
        primary_key (str, optional): Description
        id_field (str, optional): Description
    
    Returns:
        TYPE: Description
    """
    now = arrow.now().timestamp

    for record in records:
        if not record.get("record_id"):
            record["record_id"] = "{}_{}".format(record[primary_key], now)

    return records


def add_timestamps(records, timestamp_field="processed_datetime"):
    """Summary
    
    Args:
        records (TYPE): Description
        timestamp_field (str, optional): Description
    
    Returns:
        TYPE: Description
    """
    now = arrow.now().timestamp

    for record in records:
        record[timestamp_field] = now

    return records


def main():
    """Summary
    
    Args:
        jobs (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Description
    """
    # get current traffic signal data from Socrata
    socr = socratautil.Soda(resource=SOCR_SIG_RES_ID)
    signal_data = socr.data

    kits_query = kitsutil.status_query()

    kits_data = kitsutil.data_as_dict(KITS_CREDENTIALS, kits_query)

    kits_data = datautil.replace_timezone(kits_data, ["OPERATION_STATE_DATETIME"])

    kits_data = datautil.stringify_key_values(kits_data)

    #  verify the KITS data is current
    #  sometimes the signal status service goes down
    #  in which case contact ATMS support
    stale = kitsutil.check_for_stale(kits_data, "OPERATION_STATE_DATETIME")

    #  filter KITS data for statuses of concern
    kits_data = datautil.filter_by_val(kits_data, "OPERATION_STATE", FLASH_STATUSES)

    #  append kits data to signal data
    if kits_data:
        new_data = datautil.lower_case_keys(kits_data)

        new_data = datautil.merge_dicts(
            signal_data,
            new_data,
            "signal_id",
            ["operation_state_datetime", "operation_state", "plan_id"],
        )

        new_data = datautil.stringify_key_values(new_data)

    else:
        new_data = []

    #  get current signal status DATASET and metadata from socrata
    sig_status = socratautil.Soda(resource=SOCR_SIG_STAT_RES_ID)

    #  add special socrata deleted field
    #  required for sending delete requests to socrata
    fieldnames = sig_status.fieldnames + [":deleted"]

    #  transform signal status socrata data for comparison
    #  with "new" data from kits
    sig_status_data = datautil.reduce_to_keys(sig_status.data, fieldnames)
    date_fields = sig_status.date_fields
    sig_status_data = socratautil.strip_geocoding(sig_status_data)
    sig_status_data = datautil.stringify_key_values(sig_status_data)

    #  identify signals whose status (OPERATION_STATE) has changed
    cd_results = datautil.detect_changes(
        sig_status_data,
        new_data,
        "signal_id",
        #  only a change in operation state
        #  triggers an update to socrata DATASET
        keys=["operation_state"],
    )

    if cd_results["new"] or cd_results["change"] or cd_results["delete"]:

        adds = add_ids(cd_results["new"])

        deletes = socratautil.prepare_deletes(cd_results["delete"], "signal_id")

        payload = adds + cd_results["change"]

        payload = add_timestamps(payload)

        payload = payload + deletes

        payload = datautil.reduce_to_keys(payload, fieldnames)

        results = socratautil.Soda(
            auth=SOCRATA_CREDENTIALS,
            records=payload,
            resource=SOCR_SIG_STAT_RES_ID,
            date_fields=None,
            lat_field="location_latitude",
            lon_field="location_longitude",
            location_field="location",
            replace=False,
            source="kits"
        )

        return len(payload)

    else:
        return 0


if __name__ == "__main__":
    main()
