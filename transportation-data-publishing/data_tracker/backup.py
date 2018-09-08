"""
Backup knack records as CSV files.
"""
import os
import pdb

import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *

from tdutils import argutil
from tdutils import datautil

import requests


def mask_objects(app_name):

    obj_count = knackpy.get_app_data(KNACK_CREDENTIALS[app_name]["app_id"])

    obj_all = list(obj_count["counts"].keys())

    no_backup = [
        "object_137",
        "object_138",
        "object_95",
        "object_67",
        "object_77",
        "object_148",
        "object_7",
        "object_83",
        "object_58",
        "object_10",
        "object_19",
        "object_20",
        "object_24",
        "object_57",
        "object_65",
        "object_68",
        "object_76",
        "object_97",
        "object_151",
        "object_152",
        "object_155",
    ]

    objects_for_backup = [x for x in obj_all if x not in no_backup and "object_" in x]

    return objects_for_backup


def main(job):

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

    script_name = os.path.basename(__file__).replace(".py", "")

    logfile = f"{LOG_DIRECTORY}/{script_name}"
    logger = logutil.timed_rotating_log(logfile)
    logger.info("START AT {}".format(arrow.now()))

    app_name = "data_tracker_prod"
    objects = mask_objects(app_name)

    try:
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source="knack",
            destination="csv",
            auth=JOB_DB_API_TOKEN,
        )

        results = main(job)

        if results:
            job.result("success")
            logger.info("END AT {}".format(arrow.now()))

    except Exception as e:
        logger.error(str(e))
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            "Data Bakup Exception",
            str(e),
            EMAIL["user"],
            EMAIL["password"],
        )

        job.result("error", message=str(e))

        raise e
