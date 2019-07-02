"""
Write knack records to JSON.
"""

import json
import os
import pdb

import knackpy
import argutil
import datautil

import _setpath
from config.knack.config import cfg
from config.secrets import *


def cli_args():

    parser = argutil.get_parser(
        "knack_json.py", "Write Knack records to JSON.", "dataset", "app_name"
    )

    args = parser.parse_args()

    return args


def set_workdir():
    #  set the working directory to the location of this script
    #  ensures file outputs go to their intended places when
    #  script is run by an external  fine (e.g., the launcher)
    path = os.path.dirname(__file__)

    if path:
        # path will be empty if script is run from file location
        os.chdir(path)


def main():
    
    set_workdir()

    args = cli_args()

    app_name = args.app_name

    dataset = args.dataset

    fields = cfg[dataset]["to_json_fields"]

    knack_creds = KNACK_CREDENTIALS[app_name]

    #  get data from Knack
    kn = knackpy.Knack(
        obj=cfg[dataset]["obj"],
        scene=cfg[dataset]["scene"],
        view=cfg[dataset]["view"],
        ref_obj=cfg[dataset]["ref_obj"],
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
    )

    out_dir = JSON_DESTINATION

    json_data = datautil.reduce_to_keys(kn.data, fields)
    
    filename = f"{out_dir}/{dataset}.json"

    with open(filename, "w") as of:
        json.dump(json_data, of)

    return len(json_data)


if __name__ == "__main__":
    main()
