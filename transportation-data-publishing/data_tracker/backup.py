# Backup knack records as CSV files into data folder.

import os
import pdb

import arrow
import knackpy

from config.secrets import *

from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil


def mask_objects(app_name):
    """mask "no backup" objects from the list of objects that will be backupped
    
    Args:
        app_name (str): name of the application [data_tracker_prod, data_tracker_test etc.]
    
    Returns:
        list: a list of object ID exclude objects listed in no_backup list
    """
    obj_count = knackpy.get_app_data(KNACK_CREDENTIALS[app_name]["app_id"])

    obj_all = list(obj_count["counts"].keys())

    no_backup = [
        "object_137", # admin_field_meta
        "object_138", # admin_object_meta
        "object_95", # csr_flex_notes
        "object_67", # quote_of_the_week
        "object_77", # signal_id_generator     
        "object_148", # street_names 
        "object_7", # street_segments
        "object_83", # tmc_issues
        "object_58", # tmc_issues_DEPRECTATED_HISTORICAL_DATA_ONLY
        "object_10", # Asset editor
        "object_19", # Viewer
        "object_20", # System Administrator
        "object_24", # Program Editor
        "object_57", # Supervisor | AMD
        "object_65", # Technician|AMD
        "object_68", # Quote of the Week Editor
        "object_76", # Inventory Editor
        "object_97", # Account Administrator
        "object_151", # Supervisor | Signs&Markings
        "object_152", # Technician | Signs & Markings
        "object_155", # Contractor | Detection
    ]

    objects_for_backup = [x for x in obj_all if x not in no_backup and "object_" in x]

    return objects_for_backup


def main(job, **kwargs):
    """Summary
    
    Args:
        job (job class): a job class created by job util
        **kwargs (dict): All arguements include user input and arguments from public
        dictionary file
    
    Returns:
        int: number of objects that has been backup
    """
    objects = kwargs["objects"]
    app_name = kwargs["app_name"]

    job.start()

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
