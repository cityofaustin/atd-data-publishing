
# Assign detection status to traffic signal based on status of its detectors. 
# Update detection status log when signal detection status changes.

# #TODO
# - only process modified signals/detectors (currently processing all detectors)

# Attributes
# ----------
# config_detectors : dict
#     Description
# config_signals : dict
#     Description
# config_status_log : dict
#     Description
# DET_DATE_LABEL : str
#     Description
# DET_STATUS_LABEL : str
#     Description
# fieldmap_status_log : TYPE
#     Description
# SIG_DATE_LABEL : str
#     Description
# SIG_STATUS_LABEL : str
#     Description

import argparse
from collections import defaultdict
import logging
import os
import traceback
import pdb

import arrow
import knackpy

from config.secrets import *
from config.public import *

from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

# define config arguments

config_detectors = {"scene": "scene_468", "view": "view_1333", "objects": ["object_98"]}

config_signals = {"scene": "scene_73", "view": "view_197", "objects": ["object_12"]}

config_status_log = {"objects": ["object_102"]}

fieldmap_status_log = {
    "EVENT": "field_1576",
    "SIGNAL": "field_1577",
    "EVENT_DATE": "field_1578",
}

DET_STATUS_LABEL = "DETECTOR_STATUS"
DET_DATE_LABEL = "MODIFIED_DATE"

SIG_STATUS_LABEL = "DETECTION_STATUS"
SIG_DATE_LABEL = "DETECTION_STATUS_DATE"


def groupBySignal(detector_data):
    """
    Group signal detector status and status date according to parent signal. 
    
     Parameters
    ----------
    detector_data : list | (required)
        List of dicts where each dict contains vehicle detector data
        retrieved from Knack view. attributes include signal id, detector
        status, and detector status date. 
    
    Returns
    -------
    det_status : dict
        Each key in dict is a traffic signal id with keys statuses (an array of detector
        statuses) and dates (an array of detector status dates). This dict will be used to
        determine most current status and date. see methond getStatus().
    
    Parameters
    ----------
    detector_data : TYPE
        Description
    """
    det_status = defaultdict(dict)

    for det in detector_data:
        if "SIGNAL_ID" in det and DET_STATUS_LABEL in det and DET_DATE_LABEL in det:
            sig = "${}".format(det["SIGNAL_ID"])  #  format signal ID as string
            status = det[DET_STATUS_LABEL]
            status_date = det[DET_DATE_LABEL]

            if sig not in det_status:
                det_status[sig]["statuses"] = [status]
                det_status[sig]["dates"] = [status_date]
            else:
                det_status[sig]["statuses"].append(status)
                det_status[sig]["dates"].append(status_date)

    return det_status


def getStatus(sig, det_status):
    """
    Determine a signal's detection status based on the status
    of its detectors
    
    Parameters
    ----------
    sig : dict | (required)
        A signal record dict generated from a Knack.View instance
    det_status : dict | (required)
        A lookup dictionary generated from method groupBySignal()
    
    Returns
    -------
    value : string
        A detection status string of BROKEN, UNKNOWN, NO DETECTIO, OK
    """
    sig_id = "${}".format(sig["SIGNAL_ID"])

    if sig_id in det_status:
        #  any broken detector, status is BROKEN
        if "BROKEN" in det_status[sig_id]["statuses"]:
            return "BROKEN"
        #  any unknown detector, status is UNKNOWN
        if "UNKNOWN" in det_status[sig_id]["statuses"]:
            return "UNKNOWN"
        #  detection has been removed and not updated, or who knows what
        if "OK" not in det_status[sig_id]["statuses"]:
            return "UNKNOWN"
        #  detection must be OK
        return "OK"
    else:
        #  no detectors at signal
        return "NO DETECTION"


def getMaxDate(sig, det_status):
    """
    Determine a signal's most recent status date status 
    
    Parameters
    ----------
    sig : dict | (required)
        A signal record dict generated from a Knack.View instance
    det_status : dict | (required)
        A lookup dictionary generated from method groupBySignal()
    
    Returns
    -------
    value : int
        A timestamp of the maximum (ie most recent) detection status date
    """
    sig_id = "${}".format(sig["SIGNAL_ID"])

    if sig_id in det_status:
        return max([int(t) for t in det_status[sig_id]["dates"]])
    else:
        return arrow.now().format("MM-DD-YYYY")


def main(job, **kwargs):
    """Summary
    
    Parameters
    ----------
    job : Job class from job utils
        a job class that handles job method to post information to job server
    **kwargs : dict
        **kwargs: a dictionary of arguements from user input and the public.py
        dictionary file
    
    Returns
    -------
    count_sig
        number of signals that has been updated
    """
    app_name = kwargs["app_name"]

    api_key = KNACK_CREDENTIALS[app_name]["api_key"]
    app_id = KNACK_CREDENTIALS[app_name]["app_id"]

    detectors = knackpy.Knack(
        scene=config_detectors["scene"],
        view=config_detectors["view"],
        ref_obj=config_detectors["objects"],
        api_key=api_key,
        app_id=app_id,
        timeout=30,
    )

    signals = knackpy.Knack(
        scene=config_signals["scene"],
        view=config_signals["view"],
        ref_obj=config_signals["objects"],
        api_key=api_key,
        app_id=app_id,
        timeout=30,
    )

    signals.data = datautil.filter_by_key_exists(signals.data, "SIGNAL_STATUS")
    signals.data = datautil.filter_by_val(signals.data, "SIGNAL_STATUS", ["TURNED_ON"])

    lookup = groupBySignal(detectors.data)

    count_sig = 0
    count_status = 0

    for sig in signals.data:

        old_status = None
        new_status = getStatus(sig, lookup)
        new_status_date = getMaxDate(sig, lookup)

        if SIG_STATUS_LABEL in sig:
            old_status = sig[SIG_STATUS_LABEL]

            if old_status == new_status:
                continue

        payload_signals = {
            "id": sig["id"],
            SIG_STATUS_LABEL: new_status,
            SIG_DATE_LABEL: getMaxDate(sig, lookup),
        }

        payload_signals = datautil.replace_keys([payload_signals], signals.field_map)

        #  update signal record with detection status and date
        res = knackpy.record(
            payload_signals[0],
            obj_key=config_signals["objects"][0],
            app_id=app_id,
            api_key=api_key,
            method="update",
        )

        count_sig += 1

    return count_sig
