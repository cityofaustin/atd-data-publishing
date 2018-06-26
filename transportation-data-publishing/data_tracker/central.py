# This is a parent script that may used to call all data_tracker scripts. This
# parent script will handle:

# ------------------------------------------------------------------------------
# Import packages

import subprocess as sp

import argparse

import _setpath

# import credentials 
from config.secrets import *

# import variables that 

from tdutils import emailutil
from tdutils import logutil
from tdutils import argutil
from tdutils import jobutil

# a dictionary that contains information about each scripts
SCRIPTINFO = {
    "backup": {
        "arguments": None},
    "detection_status_signals":
        {
            "arguments": ["app_name"]
        },
    "device_status":
        {
            "arguments": ["device_type", "app_name"]
        },
    "device_status_log":
        {
            "arguments": ["device_type", "app_name"]
        },
    "dms_msg_pub":
        {
            "arguments": None
        },
    "esb_xml_gen":
        {
            "arguments": ["app_name"]
        },
    "esb_xml_send":
        {
            "arguments": ["app_name"]
        },
    "fulc":
        {
            "arguments": None
        },
    "kits_cctv_push":
        {
            "arguments": ["app_name"]
        },
    "location_updater":
        {
            "arguments": ["app_name"]
        },
    "markings_agol":
        {
            "arguments": ["app_name"]
        },
    "metadata_updater":
        {
            "arguments": ["app_name"]
        },
    "secondary_signals_updater":
        {
            "arguments": ["app_name"]
        },
    "signal_pm_copier":
        {
            "arguments": ["app_name"]
        },
    "signal_request_ranker":
        {
            "arguments": ["eval_type", "app_name"]
        },
    "street_seg_updater":
        {
            "arguments": None
        },
    "task_orders":
        {
            "arguments": None
        },
    "tcp_business_days":
        {
            "arguments": None
        },
    "traffic_reports":
        {
            "arguments": None
        }
}

# scriptname = "backup"
# scriptname = 

print(type(SCRIPTINFO["signal_request_ranker"]["arguments"]))


def read_arg():
    parser = argparse.ArgumentParser()

    parser.add_argument("scriptname",
                        help = "name of the script",
                        type = str,
                        )
    parser.add_argument("app_name",
                        nargs='?',
                        type = str,
                        default="")
    parser.add_argument("device_type",
                        nargs='?',
                         type = str,
                        default="")

    args = parser.parse_args()

    # determin weather enough parameters are given

    # if SCRIPTINFO[scriptname]["arguments"] != None:
    #     if len(args) != SCRIPTINFO[scriptname]["arguments"]:
    #         print("More arguments needed, expecting")
    #         raise e
    #     else

    return args

def assemble_bash(args):
    scriptname = args.scriptname
    app_name = args.app_name
    device_type = args.device_type
    bashcommand = f"python {scriptname}.py {app_name} {device_type}"
    return bashcommand


# def create_job():
#
#     job = jobutil.Job(
#         name=script_id,
#         url=JOB_DB_API_URL,
#         source='knack',
#         destination='knack',
#         auth=JOB_DB_API_TOKEN)
#
#     return job

def create_logger():

    return logger



if __name__ == "__main__":
    args = read_arg()
    # print("passed main name test")
    # print(args.scriptname)
    # print(assemble_bash(args))

    bashcommand = assemble_bash(args)

    try:

        sp.run(bashcommand, shell = True).returncode

    except Exception as e:

        logger.error(str(e))

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'Data Bakup Exception',
            str(e), EMAIL['user'],
            EMAIL['password'])

        job.result('error', message=str(e))



