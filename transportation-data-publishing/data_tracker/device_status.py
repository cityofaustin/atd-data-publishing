"""
Ping network devices and update ip comm status in Knack database.
"""
from multiprocessing.dummy import Pool as ThreadPool
import os
from os import system as system_call
from platform import system as system_name
import pdb
import socket

import arrow
import knackpy
import argutil
import datautil

import _setpath
from config.knack.config import cfg
from config.secrets import *


def ping_ip(ip, timeout):
    """
    Ping an IP address
    https://stackoverflow.com/questions/2953462/pinging-servers-in-python
    
    Args:
        ip (str): ip address
        timeout (int, optional): length of time for the function to return
        timeout status. 
    
    Returns:
        str: OFFLINE if the device is OFFLINE
             ONLINE if the device is ONLINE 
    """
    if system_name().lower() == "windows":
        #  -w is timeout -n is number of packets
        params = "-w {} -n 1".format(
            timeout * 1000
        )  # convert seconds to mills for non-windows

    else:
        #  -W is timeout -c is number of packets
        params = "-W {} -c 1".format(timeout)

    response = system_call("ping " + params + " " + ip)

    # logger.debug(str(response))

    if response != 0:
        return "OFFLINE"
    else:
        return "ONLINE"


def open_socket(ip, timeout, port=8902):

    with socket.socket() as s:
        print(ip)
        try:
            s.settimeout(timeout)
            s.connect((ip, port))
            return "ONLINE"
        except OSError:
            return "OFFLINE"


def get_status(device):
    #  get old IP status, setting it to NO COMMUNICATION if not present
    state_previous = device.setdefault("IP_COMM_STATUS", "NO COMMUNICATION")

    ip_field = device["ip_field"]
    ip = device.get(ip_field)
    device_type = device["device_type"]

    if ip:
        if device_type != "gridsmart":
            state_new = ping_ip(device[ip_field], timeout)

        else:
            """
            Gridsmart default port is 8902
            """
            state_new = open_socket(device[ip_field], timeout)

    else:
        #  set to NO COMMUINICATION if no IP address
        state_new = "NO COMMUNICATION"

    if state_previous != state_new:

        device["IP_COMM_STATUS"] = state_new
        #  timestamps into and out of knack are naive
        #  so we create a naive local timestamp by replacing
        #  a localized timestamp's timezone info with UTC
        device["COMM_STATUS_DATETIME_UTC"] = datautil.local_timestamp()

        return device

    else:
        return None


def apply_modified_date(dicts, key="MODIFIED_DATE", offset=600000):
    #  set the record modified date as a "local" timestamp (knack-friendly)
    #  also apply a forward offset to ensure modified records are picked up
    #  by the publishing scripts which are checking for recently modded data
    for record in dicts:
        record[key] = datautil.local_timestamp() + offset

    return dicts


def apply_modified_by(dicts, key="MODIFIED_BY", user_id="5bbd6eaa7810a00b02a87bcd"):
    #  set the record modified by field. The default value applies a generica API
    #  user name
    for record in dicts:
        record[key] = user_id

    return dicts


def set_workdir():
    #  set the working directory to the location of this script
    #  ensures file outputs go to their intended places when
    #  script is run by an external  fine (e.g., the launcher)
    path = os.path.dirname(__file__)
    os.chdir(path)


def cli_args():

    parser = argutil.get_parser(
        "device_status_check.py",
        "Ping network devices to verify connenectivity.",
        "device_type",
        "app_name",
        "--replace",
    )

    args = parser.parse_args()

    return args


def main():

    args = cli_args()

    device_type = args.device_type
    app_name = args.app_name

    primary_key = cfg[device_type]["primary_key"]
    ip_field = cfg[device_type]["ip_field"]

    global timeout
    timeout = cfg[device_type].get("timeout")

    if not timeout:
        timeout = 3

    knack_creds = KNACK_CREDENTIALS[app_name]

    out_fields_upload = [
        "id",
        ip_field,
        "IP_COMM_STATUS",
        "COMM_STATUS_DATETIME_UTC",
        "MODIFIED_DATE",
        "MODIFIED_BY",
    ]

    #  get device data from Knack application
    kn = knackpy.Knack(
        obj=cfg[device_type]["obj"],
        scene=cfg[device_type]["scene"],
        view=cfg[device_type]["view"],
        ref_obj=cfg[device_type]["ref_obj"],
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
    )

    #  append config data to each item to be processed
    #  this is a hacky way to pass args to each thread
    for i in kn.data:
        i["ip_field"] = ip_field
        i["device_type"] = device_type

    pool = ThreadPool(8)

    results = pool.map(get_status, kn.data)

    for result in results:
        """
        Result is None if status has not changed. Otherwise result
        is device record dict
        """
        if result:
            #  format for upload to Knack
            result = [result]
            result = apply_modified_date(result)
            result = apply_modified_by(result)
            result = datautil.reduce_to_keys(result, out_fields_upload)
            result = datautil.replace_keys(result, kn.field_map)

            res = knackpy.record(
                result[0],
                obj_key=cfg[device_type]["ref_obj"][
                    0
                ],  #  assumes record object is included in config ref_obj and is the first elem in array,
                app_id=knack_creds["app_id"],
                api_key=knack_creds["api_key"],
                method="update",
            )

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    return len([record for record in results if record])


if __name__ == "__main__":
    main()
