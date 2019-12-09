"""
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus
"""
import os
import pdb
import time

import arrow
import knackpy
import requests

import _setpath
from config.esb.config import cfg as CONFIG
from config.secrets import *
import argutil
import datautil


def get_record_id_from_file(directory, file):
    """
    Extract Knack record id from filename.
    
    Expects XML messages to be named with the app name, incremental record ID, as well as
    Knack database ID. The former is used to sort records in chronological
    order (not returned by this function) and the latter is used to update
    the Knack record with a 'SENT' status when message has been successfully
    transmitted to ESB.
    
    Expected format is incrementaId_knackId.xml. E.g. data_tracker_prod_10034_axc3345f23msf0.xml
    
    Args:
        directory (TYPE): Description
        file (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    record_data = file.split(".")[0]
    return record_data.split("_-_")[2]


def get_sorted_file_list(path, app_name):
    """
    Retrieve XML files from directory and return a sorted list of
    files based on filename.
    
    Assumes ascendant sorting of filenames is equivalent to sorting
    oldest to newest records in database. This is accomplished by naming files
    with their incremental record ID via esb_xml_gen.py
    
    Returns array of filenames sorted A-Z, aka oldest to newest.
    
    Args:
        path (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    files = []

    for file in os.listdir(path):
        filename = os.fsdecode(file)

        if app_name in filename and filename.endswith(".xml"):
            files.append(file)

    files.sort()

    return files


def get_msg(directory, file):
    """Summary
    
    Args:
        directory (TYPE): Description
        file (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    fin = os.path.join(directory, file)
    with open(fin, "r") as msg:
        return msg.read()


def send_msg(msg, endpoint, path_cert, path_key, timeout=20):
    """Summary
    
    Args:
        msg (TYPE): Description
        endpoint (TYPE): Description
        path_cert (TYPE): Description
        path_key (TYPE): Description
        timeout (int, optional): Description
    
    Returns:
        TYPE: Description
    """
    headers = {"content-type": "text/xml"}
    res = requests.post(
        endpoint,
        data=msg,
        headers=headers,
        timeout=timeout,
        verify=False,
        cert=(path_cert, path_key),
    )

    return res


def move_file(old_dir, new_dir, f):
    infile = os.path.join(old_dir, f)
    outfile = os.path.join(new_dir, f)
    os.rename(infile, outfile)
    return True


def create_payload(record_id, status_field):
    payload = {"id": record_id, status_field: "SENT"}
    return payload


def set_workdir():
    #  set the working directory to the location of this script
    #  ensures file outputs go to their intended places when
    #  script is run by an external  fine (e.g., the launcher)
    path = os.path.dirname(__file__)

    if path:
        os.chdir(path)


def cli_args():
    parser = argutil.get_parser(
        "esb_xml_send.py",
        "Update service requests in the CSR system from Knack via Enterprise Service Bus",
        "app_name",
    )

    args = parser.parse_args()

    return args


def main():
    set_workdir()

    args = cli_args()

    app_name = args.app_name

    knack_creds = KNACK_CREDENTIALS[app_name]
    
    # TODO: add a dedicated arg for source
    if "data_tracker" in app_name:
        source = "tmc_activities"
    elif "signs" in app_name:
        source = "signs_markings_activities"
    
    cfg = CONFIG[source]

    base_path = os.path.abspath(ESB_XML_DIRECTORY)
    inpath = "{}/{}".format(base_path, "ready_to_send")
    outpath = "{}/{}".format(base_path, "sent")

    if not os.path.exists(inpath):
        os.makedirs(inpath)

    if not os.path.exists(outpath):
        os.makedirs(outpath)

    directory = os.fsencode(inpath)
    """
    Get files in order by incremental ID. This ensures messages
    are transmitted chronologically.
    """
    files = get_sorted_file_list(inpath, app_name)

    for filename in files:
        """
        Extract record id, send message to ESB, move file to 'sent' folder,
        and update Knack record with status of SENT.
        """
        record_id = get_record_id_from_file(inpath, filename)

        msg = get_msg(inpath, filename)

        res = send_msg(msg, ESB_ENDPOINT["prod"], cfg["path_cert"], cfg["path_key"])

        res.raise_for_status()

        payload = create_payload(record_id, cfg["esb_status_field"])

        res = knackpy.record(
            payload,
            obj_key=cfg["obj"],
            app_id=knack_creds["app_id"],
            api_key=knack_creds["api_key"],
            method="update",
        )

        move_file(inpath, outpath, filename)

        # wait a few seconds between between message send
        # hoping to ensure messages are processed chronologically
        # by 311
        time.sleep(10)

    return len(files)


if __name__ == "__main__":

    main()
