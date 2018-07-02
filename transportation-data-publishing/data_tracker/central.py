# This is a parent script that may used to call all data_tracker scripts. This
# parent script will handle:

# ------------------------------------------------------------------------------
# Import packages

import subprocess as sp
import argparse
import arrow

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
    "backup":
        {
            "arguments": None,
            "objects": ['object_87', 'object_93', 'object_77', 'object_53',
                        'object_96', 'object_83', 'object_95', 'object_21',
                        'object_14', 'object_109', 'object_73', 'object_110',
                        'object_15', 'object_36', 'object_11', 'object_107',
                        'object_115', 'object_116', 'object_117', 'object_67',
                        'object_91', 'object_89', 'object_12', 'object_118',
                        'object_113', 'object_98', 'object_102', 'object_71',
                        'object_84', 'object_13', 'object_26', 'object_27',
                        'object_81', 'object_82', 'object_7', 'object_42',
                        'object_43', 'object_45', 'object_75', 'object_58',
                        'object_56', 'object_54', 'object_86', 'object_78',
                        'object_85', 'object_104', 'object_106', 'object_31',
                        'object_101', 'object_74', 'object_94', 'object_9',
                        'object_10', 'object_19', 'object_20', 'object_24',
                        'object_57', 'object_59', 'object_65', 'object_68',
                        'object_76', 'object_97', 'object_108', 'object_140',
                        'object_142', 'object_143', 'object_141', 'object_149']

        },
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

def create_logger(scriptname):

    logfile = f'{LOG_DIRECTORY}/{scriptname}.log'

    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format(arrow.now()))

    return logger



if __name__ == "__main__":
    args = read_arg()

    scriptname = args.scriptname

    # print("passed main name test")
    # print(args.scriptname)
    # print(assemble_bash(args))

    bashcommand = assemble_bash(args)
    logger = create_logger(scriptname)

    try:

        exitcode = sp.run(bashcommand, shell = True).returncode

    except Exception as e:

        logger.error(str(e))

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'Data Bakup Exception',
            str(e),
            EMAIL['user'],
            EMAIL['password'])



