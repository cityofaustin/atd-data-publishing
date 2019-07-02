"""
Extract 311 SR flex notes and archive in postgresql vis postgREST.
"""

import pdb

import arrow
import knackpy
from pypgrest import Postgrest
import argutil
import datautil
import knackutil


import _setpath
from config.knack.config import cfg
from config.secrets import *


def knackpy_wrapper(cfg_dataset, auth, filters=None):
    return knackpy.Knack(
        scene=cfg_dataset["scene"],
        view=cfg_dataset["view"],
        ref_obj=cfg_dataset["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters
    )


def filter_by_date(data, date_field, compare_date):
    """
    Date field and compare date should be unix timestamps with mills
    """
    return [record for record in data if record[date_field] >= compare_date]



def main():

    args = cli_args()
    
    auth = KNACK_CREDENTIALS[args.app_name]

    cfg_dataset = cfg[args.dataset]

    filters = knackutil.date_filter_on_or_after(
        args.last_run_date, cfg_dataset["modified_date_field_id"]
    )

    kn = knackpy_wrapper(cfg_dataset, auth, filters=filters)

    if not kn.data:
        return 0

    # Filter data for records that have been modifed after the last
    # job run
    last_run_timestamp = arrow.get(args.last_run_date).timestamp * 1000

    kn.data = filter_by_date(
        kn.data, cfg_dataset["modified_date_field"], last_run_timestamp
    )

    if not kn.data:
        return 0

    pgrest = Postgrest(cfg_dataset["pgrest_base_url"], auth=JOB_DB_API_TOKEN)

    for record in kn.data:
        # convert mills timestamp to iso
        record[cfg_dataset["modified_date_field"]] = arrow.get((record[cfg_dataset["modified_date_field"]] / 1000)).format()

    kn.data = datautil.lower_case_keys(kn.data)
    
    pgrest.upsert(kn.data)

    return len(kn.data)


def cli_args():
    parser = argutil.get_parser(
        "pgrest_data_pub.py",
        "Publish Knack Data to postgREST.",
        "dataset",
        "app_name",
        "--last_run_date",
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
