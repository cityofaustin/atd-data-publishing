
# Assign detection status to traffic signal based on status of its detectors.
# Update detection status log when signal detection status changes.

# #TODO
# - only process modified signals/detectors (currently processing all detectors)

import argparse
from collections import defaultdict
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from config.knack.config import DETETECTION_STATUS_SIGNALS as cfg

import argutil
import datautil


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
        if (
            "SIGNAL_ID" in det
            and cfg["DET_STATUS_LABEL"] in det
            and cfg["DET_DATE_LABEL"] in det
        ):
            sig = "${}".format(det["SIGNAL_ID"])  #  format signal ID as string
            status = det[cfg["DET_STATUS_LABEL"]]
            status_date = det[cfg["DET_DATE_LABEL"]]

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


def cli_args():

    parser = argutil.get_parser(
        "detection_status_signals.py",
        "Assign detection status to signals based on status of its detectors.",
        "app_name",
    )

    parsed = parser.parse_args()

    return parsed


def main():
    """Summary
    
    Parameters
    ----------
    None
    
    Returns
    -------
    count_sig
        number of signals that has been updated
    """
    args = cli_args()
    app_name = args.app_name

    api_key = KNACK_CREDENTIALS[app_name]["api_key"]
    app_id = KNACK_CREDENTIALS[app_name]["app_id"]

    detectors = knackpy.Knack(
        scene=cfg["CONFIG_DETECTORS"]["scene"],
        view=cfg["CONFIG_DETECTORS"]["view"],
        ref_obj=cfg["CONFIG_DETECTORS"]["objects"],
        api_key=api_key,
        app_id=app_id,
        timeout=30,
    )

    signals = knackpy.Knack(
        scene=cfg["CONFIG_SIGNALS"]["scene"],
        view=cfg["CONFIG_SIGNALS"]["view"],
        ref_obj=cfg["CONFIG_SIGNALS"]["objects"],
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

        if cfg["SIG_STATUS_LABEL"] in sig:
            old_status = sig[cfg["SIG_STATUS_LABEL"]]

            if old_status == new_status:
                continue

        payload_signals = {
            "id": sig["id"],
            cfg["SIG_STATUS_LABEL"]: new_status,
            cfg["SIG_DATE_LABEL"]: getMaxDate(sig, lookup),
        }

        payload_signals = datautil.replace_keys([payload_signals], signals.field_map)

        #  update signal record with detection status and date
        res = knackpy.record(
            payload_signals[0],
            obj_key=cfg["CONFIG_SIGNALS"]["objects"][0],
            app_id=app_id,
            api_key=api_key,
            method="update",
        )

        count_sig += 1

    return count_sig


if __name__ == "__main__":
    main()
