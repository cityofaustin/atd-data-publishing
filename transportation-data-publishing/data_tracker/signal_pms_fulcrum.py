"""Summary
This script will transfer signal preventive maintenance work orders from postgre
sql database to knack preventive maintenance work order object.
Attributes:
    form_id (str): Description
    key (TYPE): Description
    pgrest (TYPE): Description
"""
import pandas as pd
import numpy as np
import knackpy
import fulcrum as fc
import requests
import pdb
import json
from datetime import datetime, timedelta, date
import time
import copy

import _setpath
from config.secrets import *
from config.knack.config import SIGNAL_PMS_POSTGRE_KNACK as cfg

# from tdutils.pgrestutil import Postgrest
import argutil, datautil
from pypgrest import Postgrest


key = FULCRUM.get("api_key")

# create postgrest instance

pgrest = Postgrest(cfg.get("postgre_url"), auth=JOB_DB_API_TOKEN)


def get_postgre_records():
    """Summary
    get postgreSQL records for all signal pms currently in postgreSQL
    
    Returns:
        list: list of dictionaries of all pgrest records
    """
    params = {}

    postgre_records = pgrest.select(params=params)
    postgre_records_df = pd.DataFrame.from_dict(postgre_records)

    # temporary fix to remove duplicate pm records
    postgre_records_df = postgre_records_df.sort_values(
        "modified_date"
    ).drop_duplicates(subset="fulcrum_id", keep="last")

    postgre_records = postgre_records_df.to_dict(orient="records")

    return postgre_records


def get_knack_pm_records(app_name):
    """get preventive maintenance data from knack preventive maintenance object

    Returns:
        knacypy obj: Description
    """

    signals_pms_fulcrum = knackpy.Knack(
        scene=cfg["knack_pms"]["scene"],
        view=cfg["knack_pms"]["view"],
        ref_obj=cfg["knack_pms"]["ref_obj"],
        api_key=KNACK_CREDENTIALS[app_name]["api_key"],
        app_id=KNACK_CREDENTIALS[app_name]["app_id"],
        timeout=30,
    )

    return signals_pms_fulcrum


def get_signals_records(app_name):
    """get all signal records from knack signal object.

    Returns:
        knackpy object: Description
    """
    signals_knack_id_dict = knackpy.Knack(
        scene=cfg["knack_signals"]["scene"],
        view=cfg["knack_signals"]["view"],
        ref_obj=cfg["knack_signals"]["ref_obj"],
        api_key=KNACK_CREDENTIALS[app_name]["api_key"],
        app_id=KNACK_CREDENTIALS[app_name]["app_id"],
        timeout=30,
    )

    return signals_knack_id_dict


def get_technicians_records(app_name):
    signals_knack_id_dict = knackpy.Knack(
        obj=cfg["knack_technicians"]["objects"],
        app_id=KNACK_CREDENTIALS[app_name]["app_id"],
        api_key=KNACK_CREDENTIALS[app_name]["api_key"],
    )

    return signals_knack_id_dict


def cli_args():
    """Summary
    
    Returns:
        TYPE: Description
    """
    parser = argutil.get_parser(
        "signals_pms_fulcrum.py",
        "transfer signal preventive maintenance records from postgre sql to knack",
        "app_name",
        "--replace",
        "--last_run_date",
    )

    args = parser.parse_args()

    return args


def get_last_run(args, knack_records):
    """Summary: get the most recent modified date from all knack
    signal_pm object

    Args:
        args (namespace): command line inputs
        knack_records (knack object): a list of all knack pm records
        with fulcrum id

    Returns:

    """
    knack_records_data = knack_records.data

    if args.last_run_date or args.last_run_date == 0:
        return args.last_run_date
    else:
        for record in knack_records_data:

            last_run_date_item = max(
                knack_records_data, key=lambda x: x["MODIFIED_DATE"]
            )

            return last_run_date_item["MODIFIED_DATE"]


def map_knack_id_signal_id(signals_records, payloads):
    """Summary
    
    Args:
        signals_records (knack object): all signal records from knack
        payloads (list of dictionary): map signal_id on pm payloads
    
    Returns:
        DataFrame: the prepared DataFrame
    """
    payloads = pd.DataFrame.from_dict(payloads)

    signal_records_df = pd.DataFrame.from_dict(signals_records.data)

    signal_records_df = signal_records_df[["SIGNAL_ID", "id"]]

    signal_records_df = signal_records_df.rename(columns={"id": "SIGNAL"})

    signal_records_df["SIGNAL_ID"] = signal_records_df["SIGNAL_ID"].astype(str)

    signal_id_mapped_payloads = payloads.merge(
        right=signal_records_df, left_on="signal_id", right_on="SIGNAL_ID", how="left"
    )

    signal_id_mapped_payloads = signal_id_mapped_payloads.drop(["signal_id"], axis=1)

    return signal_id_mapped_payloads


def map_technicians_id_pm_payloads(payloads, knack_technicians):
    """
    Retrieve the Knack record ID of each technician and set payload
    values accordingly.
    """
    knack_technicians = knack_technicians.data
    knack_technicians_mapped = {}

    for item in knack_technicians:
        knack_technicians_mapped[item["Email_email"]] = item["id"]

    for i, payload in enumerate(payloads):

        completed_by = payload.get("PM_COMPLETED_BY")

        if "choice_values" in completed_by:
            # multiple technicians selected, but knack expectes one "complted_by" value only
            # so we take the first

            completed_by = json.loads(completed_by)

            payloads[i]["PM_COMPLETED_BY"] = knack_technicians_mapped[
                completed_by["choice_values"][0]
            ]

        else:
            # only one technician selected
            payloads[i]["PM_COMPLETED_BY"] = knack_technicians_mapped[
                payload.get("PM_COMPLETED_BY")
            ]

    return payloads


def prepare_pm_payloads(
    last_run_date, pgrest_records, signal_records, knack_pm_records, knack_technicians
):
    """Summary
    #TODO optimize the comparison
    
    Args:
        last_run_date (UNIX Timestamp):  
        pgrest_records (knack object): Description
        signal_records (knack object): Description
        knack_pm_records (knack object): Description
    
    Returns:
        list: pms in postgre that are not in 
    
    Deleted Parameters:
        last_run_date_item (TYPE): Description
    """

    pgrest_records_df = pd.DataFrame.from_dict(pgrest_records)

    pgrest_records_df["modified_date"] = pgrest_records_df["modified_date"].apply(
        datetime_to_unix_timestamp
    )

    pgrest_records_df["PM_STATUS"] = "COMPLETED"

    pgrest_records_df = map_knack_id_signal_id(signal_records, pgrest_records_df)

    pgrest_records_df.columns = map(str.upper, pgrest_records_df.columns)

    pgrest_records_df = pgrest_records_df.rename(columns={"ID": "id"})

    pgrest_records_list = pgrest_records_df.to_dict(orient="records")

    pm_payloads = []

    for pgrest_record in pgrest_records_list:
        modified_date = pgrest_record["MODIFIED_DATE"]

        if modified_date >= last_run_date:
            pm_payloads.append(pgrest_record)

    pm_payloads = map_technicians_id_pm_payloads(pm_payloads, knack_technicians)

    return pm_payloads


def replace_pm_records(
    postgre_records, knack_pm_records, signal_records, knack_technicians, app_name
):
    """Summary
    
    Args:
        postgre_records (TYPE): Description
        knack_pm_records (TYPE): Description
        signal_records (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    postgre_records_df = pd.DataFrame.from_dict(postgre_records)
    knack_pm_records_df = pd.DataFrame.from_dict(knack_pm_records.data)

    pm_insert_payloads = postgre_records_df[
        ~postgre_records_df["fulcrum_id"].isin(knack_pm_records_df["FULCRUM_ID"])
    ].copy()

    pm_update_payloads = postgre_records_df[
        postgre_records_df["fulcrum_id"].isin(knack_pm_records_df["FULCRUM_ID"])
    ].copy()

    pm_insert_payloads["MODIFIED_DATE"] = datautil.local_timestamp()
    pm_update_payloads["MODIFIED_DATE"] = datautil.local_timestamp()

    pm_insert_payloads = map_knack_id_signal_id(signal_records, pm_insert_payloads)
    pm_update_payloads = map_knack_id_signal_id(signal_records, pm_update_payloads)

    knack_pm_records_id_df = knack_pm_records_df[["FULCRUM_ID", "id"]]
    pm_update_payloads = pm_update_payloads.merge(
        right=knack_pm_records_id_df,
        left_on="fulcrum_id",
        right_on="FULCRUM_ID",
        how="left",
    )

    pm_insert_payloads["PM_STATUS"] = "COMPLETED"
    pm_update_payloads["PM_STATUS"] = "COMPLETED"

    pm_insert_payloads.columns = map(str.upper, pm_insert_payloads.columns)
    pm_update_payloads.columns = map(str.upper, pm_update_payloads.columns)

    pm_update_payloads = pm_update_payloads.rename(columns={"ID": "id"})

    pm_insert_payloads = pm_insert_payloads.to_dict(orient="records")
    pm_update_payloads = pm_update_payloads.to_dict(orient="records")

    if len(pm_insert_payloads) != 0:
        pm_insert_payloads = map_technicians_id_pm_payloads(
            pm_insert_payloads, knack_technicians
        )

    pm_update_payloads = map_technicians_id_pm_payloads(
        pm_update_payloads, knack_technicians
    )

    # update signal modified time in replace method

    pm_replace_payloads_shallow = pm_update_payloads + pm_insert_payloads
    pm_replace_payloads = copy.deepcopy(pm_replace_payloads_shallow)

    for d in pm_replace_payloads:
        if "id" in d:
            del d["id"]

    signal_payloads = prepare_signals_payloads(pm_replace_payloads, signal_records)
    signals_payloads = datautil.replace_keys(signal_payloads, signal_records.field_map)
    signal_results = update_signals_modified_time(signals_payloads, app_name)

    # end update signal modified time in replace method

    pm_insert_payloads = datautil.replace_keys(
        pm_insert_payloads, knack_pm_records.field_map
    )

    pm_update_payloads = datautil.replace_keys(
        pm_update_payloads, knack_pm_records.field_map
    )

    for payload in pm_insert_payloads:
        print("inserting", payload)

        insert_res = knackpy.record(
            payload,
            obj_key="object_84",
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            method="create",
        )

    for payload in pm_update_payloads:
        print("updating", payload)

        update_res = knackpy.record(
            payload,
            obj_key="object_84",
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            method="update",
        )

    return len(pm_insert_payloads) + len(pm_update_payloads)


def datetime_to_unix_timestamp(dateandtime):

    return (
        int(
            time.mktime(datetime.strptime(dateandtime, "%Y-%m-%dT%H:%M:%S").timetuple())
        )
    ) * 1000


def prepare_signals_payloads(payloads, signals_records):
    """Summary
    Prepare signal payloads by change data type and map new signal information to
    the signal object. 
    Args:
        payloads (TYPE): the raw payload in list of dictionaries format 
    
    Returns:
        TYPE: a list of dictionary
    """

    # signals_records = get_signals_records()
    signals_records_df = pd.DataFrame.from_dict(signals_records.data)

    payloads = pd.DataFrame.from_dict(payloads)

    signals_records_df["SIGNAL_ID"] = signals_records_df["SIGNAL_ID"].astype("str")

    payloads["SIGNAL_ID"] = payloads["SIGNAL_ID"].astype("str")

    signals_payloads = payloads.merge(
        right=signals_records_df, left_on="SIGNAL_ID", right_on="SIGNAL_ID", how="left"
    )

    signals_payloads["MODIFIED_DATE"] = datautil.local_timestamp()

    signals_payloads = signals_payloads[["SIGNAL_ID", "MODIFIED_DATE", "id"]]

    signals_payloads = signals_payloads.rename(
        columns={"MODIFIED_DATE": "MODIFIED_DATE"}
    )

    signals_payloads = signals_payloads.to_dict(orient="records")

    return signals_payloads


def insert_pms(payloads, app_name):
    """Summary
    
    Args:
        payloads (TYPE): Description
    
    Returns:
        TYPE: Description
    """

    responses_list = []

    for payload in payloads:
        print("inserting", payload)

        response = knackpy.record(
            payload,
            obj_key="object_84",
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            method="create",
        )

        responses_list.append(response)

    return responses_list


def update_signals_modified_time(signals_payloads, app_name):
    """Summary
    
    Args:
        signals_payloads (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    responses_list = []

    responses_list = []

    for signal_payload in signals_payloads:

        response = knackpy.record(
            signal_payload,
            obj_key="object_12",
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            method="update",
        )

        responses_list.append(response)

    return responses_list


def main():
    """Summary
    
    Returns:
        TYPE: Description
    """

    args = cli_args()
    app_name = args.app_name

    pgrest_records = get_postgre_records()
    knack_records = get_knack_pm_records(app_name)
    signals_records = get_signals_records(app_name)
    knack_technicians_records = get_technicians_records(app_name)

    last_run_date = get_last_run(args, knack_records)

    if args.replace:
        signal_results = replace_pm_records(
            pgrest_records,
            knack_records,
            signals_records,
            knack_technicians_records,
            app_name,
        )

    else:
        pm_payloads = prepare_pm_payloads(
            last_run_date,
            pgrest_records,
            signals_records,
            knack_records,
            knack_technicians_records,
        )

        if len(pm_payloads) == 0:

            return 0
        else:
            signal_payloads = prepare_signals_payloads(pm_payloads, signals_records)

            pm_payloads = datautil.replace_keys(pm_payloads, knack_records.field_map)

            signals_payloads = datautil.replace_keys(
                signal_payloads, signals_records.field_map
            )

            signal_results = update_signals_modified_time(signals_payloads, app_name)

            results = insert_pms(pm_payloads, app_name)

            results = len(results)

    return signal_results


if __name__ == "__main__":
    signal_results = main()

    # args = cli_args()
    # app_name = args.app_name
    # app_name = "data_tracker_prod"

    # pgrest_records = get_postgre_records()
    # knack_records = get_knack_pm_records(app_name)
    # signals_records = get_signals_records(app_name)
    # knack_technicians_records = get_technicians_records(app_name)

    # signal_results = replace_pm_records(
    #         pgrest_records,
    #         knack_records,
    #         signals_records,
    #         knack_technicians_records,
    #         app_name,
    #     )
