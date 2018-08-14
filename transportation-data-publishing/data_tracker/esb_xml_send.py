"""
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus

Attributes:
    cfg (TYPE): Description
"""
import argparse
import os
import pdb
import traceback

import arrow
import knackpy
import requests

import _setpath
from config.esb.config import cfg
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

cfg = cfg["tmc_activities"]


def get_record_id_from_file(directory, file):
    """
    Extract Knack record id from filename.
    
    Expects XML messages to be named with incremental record ID as well as
    Knack database ID. The former is used to sort records in chronological
    order (not returned by this function) and the latter is used to update
    the Knack record with a 'SENT' status when message has been successfully
    transmitted to ESB.
    
    Expected format is incrementaId_knackId.xml. E.g. 10034_axc3345f23msf0.xml
    
    Args:
        directory (TYPE): Description
        file (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    record_data = file.split(".")[0]
    return record_data.split("_")[1]


def get_sorted_file_list(path):
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

        if filename.endswith(".xml"):
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
    """Summary
    
    Args:
        old_dir (TYPE): Description
        new_dir (TYPE): Description
        f (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    infile = os.path.join(old_dir, f)
    outfile = os.path.join(new_dir, f)
    os.rename(infile, outfile)
    return True


def create_payload(record_id):
    """Summary
    
    Args:
        record_id (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    payload = {"id": record_id, cfg["esb_status_field"]: "SENT"}
    return payload


def cli_args():
    """Summary
    
    Returns:
        TYPE: Description
    """
    parser = argutil.get_parser(
        "esb_xml_send.py",
        "Update service requests in the CSR system from Knack via Enterprise Service Bus",
        "app_name",
    )

    args = parser.parse_args()

    return args


def main(job, **kwargs):
    """Summary
    
    Args:
        job (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Description
    """
    app_name = kwargs["app_name"]
    knack_creds = KNACK_CREDENTIALS[app_name]

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
    files = get_sorted_file_list(inpath)

    for filename in files:
        """
        Extract record id, send message to ESB, move file to 'sent' folder,
        and update Knack record with status of SENT.
        """
        record_id = get_record_id_from_file(inpath, filename)

        msg = get_msg(inpath, filename)

        res = send_msg(msg, ESB_ENDPOINT["prod"], cfg["path_cert"], cfg["path_key"])

        res.raise_for_status()

        payload = create_payload(record_id)

        res = knackpy.record(
            payload,
            obj_key=cfg["obj"],
            app_id=knack_creds["app_id"],
            api_key=knack_creds["api_key"],
            method="update",
        )

        move_file(inpath, outpath, filename)

    # logger.info('{} records transmitted.'.format(len(files)))

    return len(files)


if __name__ == "__main__":
    # script_name = os.path.basename(__file__).replace('.py', '')
    # logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    #
    # logger = logutil.timed_rotating_log(logfile)
    # logger.info('START AT {}'.format( arrow.now() ))

    # args = cli_args()
    # logger.info( 'args: {}'.format( str(args) ))

    # app_name = args.app_name

    # knack_creds = KNACK_CREDENTIALS[app_name]

    base_path = os.path.abspath(ESB_XML_DIRECTORY)
    inpath = "{}/{}".format(base_path, "ready_to_send")
    outpath = "{}/{}".format(base_path, "sent")

    try:
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source="knack",
            destination="ESB",
            auth=JOB_DB_API_TOKEN,
        )

        # if not os.path.exists(inpath):
        #     os.makedirs(inpath)
        #
        # if not os.path.exists(outpath):
        #     os.makedirs(outpath)

        job.start()

        results = main()

        job.result("success", records_processed=results)

        logger.info("END AT {}".format(arrow.now()))

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(str(e))
        logger.error(error_text)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            "ESB Publication Failure",
            str(error_text),
            EMAIL["user"],
            EMAIL["password"],
        )

        job.result("error")

        raise e
