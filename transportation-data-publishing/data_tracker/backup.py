# Backup knack records as CSV files into data folder.

import os
import pdb

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *

import argutil
import datautil


def mask_objects(app_name, blacklist):
    """mask "no backup" objects from the list of objects that will be backupped
    
    Args:
        app_name (str): name of the application [data_tracker_prod, data_tracker_test etc.]
        blacklist (list): list of object keys to exclude from backup

    Returns:
        list: a list of object ID exclude objects listed in no_backup list
    """
    obj_count = knackpy.get_app_data(KNACK_CREDENTIALS[app_name]["app_id"])

    obj_all = list(obj_count["counts"].keys())

    objects_for_backup = [x for x in obj_all if x not in blacklist and "object_" in x]

    return objects_for_backup


def cli_args():

    parser = argutil.get_parser(
        "backup.py", "Backup objects from knack application to csv.", "app_name"
    )

    parsed = parser.parse_args()

    return parsed


def set_workdir():
    #  set the working directory to the location of this script
    #  ensures file outputs go to their intended places when
    #  script is run by an external  fine (e.g., the launcher)
    path = os.path.dirname(__file__)
    os.chdir(path)


def main():
    """Summary
    
    Args:
        None
    
    Returns:
        int: number of objects that has been backup
    """

    args = cli_args()

    set_workdir()

    app_name = args.app_name
    blacklist = cfg["backup"]["objects"]

    objects = mask_objects(app_name, blacklist)

    count = 0

    for obj in objects:

        kn = knackpy.Knack(
            obj=obj,
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
        )

        if kn.data:

            today = arrow.now().format("YYYY_MM_DD")

            file_name = "{}/{}_{}.csv".format(BACKUP_DIRECTORY, obj, today)

            date_fields_kn = [
                kn.fields[f]["label"]
                for f in kn.fields
                if kn.fields[f]["type"] in ["date_time", "date"]
            ]

            kn.data = datautil.mills_to_iso(kn.data, date_fields_kn)

            try:
                kn.to_csv(file_name)

            except UnicodeError:
                kn.data = [{key: str(d[key]).encode()} for d in kn.data for key in d]
                kn.to_csv(file_name)

            count += 1

        else:
            continue

    return count


if __name__ == "__main__":
    main()
