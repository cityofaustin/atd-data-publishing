"""
Update traffic signal records with secondary signal relationships
"""
import collections
import pdb

import arrow
import knackpy
import argutil

import _setpath
from config.knack.config import SECONDARY_SIGNALS_UPDATER as cfg
from config.secrets import *


def cli_args():
    parser = argutil.get_parser(
        "secondary_signals_updater.py",
        "Update traffic signal records with secondary signal relationships.",
        "app_name",
    )

    args = parser.parse_args()

    return args


def get_new_prim_signals(signals):
    """
    create a dict of primary signals and the secondary signals they control
    data is compiled from the 'primary_signal' field on secondary signals
    this field is maintained by ATD staff via the signals forms in the knack database
    
    Args:
        signals (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    signals_with_children = {}

    for signal in signals:

        try:
            #  get id of parent signal
            knack_id = signal["PRIMARY_SIGNAL"][0]["id"]
        except (IndexError, AttributeError):
            #  empty key
            continue

        if knack_id not in signals_with_children:
            #  add entry for parent signal
            signals_with_children[knack_id] = []
        #  add current signal to list of parent's children
        signals_with_children[knack_id].append(signal["id"])

    return signals_with_children


def get_old_prim_signals(signals):
    """
    create a dict of primary signals and the secondary signals they control
    data is compiled from the 'secondary_signals' field on primary signals
    this field is populated by this Python service
    
    Args:
        signals (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    signals_with_children = {}

    for signal in signals:
        knack_id = signal["id"]

        secondary_signals = []

        try:
            for secondary in signal["SECONDARY_SIGNALS"]:

                secondary_signals.append(secondary["id"])

                signals_with_children[knack_id] = secondary_signals

        except (KeyError, AttributeError):
            continue

    return signals_with_children


def main():

    args = cli_args()

    app_name = args.app_name
    knack_creds = KNACK_CREDENTIALS[app_name]

    kn = knackpy.Knack(
        scene=cfg["scene"],
        view=cfg["view"],
        ref_obj=cfg["ref_obj"],
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
        raw_connections=True,
    )

    primary_signals_old = get_old_prim_signals(kn.data)
    primary_signals_new = get_new_prim_signals(kn.data)

    payload = []

    for signal_id in primary_signals_new:
        """
        identify new and changed primary-secondary relationships
        """
        if signal_id in primary_signals_old:
            new_secondaries = collections.Counter(primary_signals_new[signal_id])
            old_secondaries = collections.Counter(primary_signals_old[signal_id])

            if old_secondaries != new_secondaries:

                payload.append(
                    {
                        "id": signal_id,
                        cfg["update_field"]: primary_signals_new[signal_id],
                    }
                )

        else:
            payload.append(
                {"id": signal_id, cfg["update_field"]: primary_signals_new[signal_id]}
            )

    for signal_id in primary_signals_old:
        """
        identify primary-secondary relationships that have been removed
        """
        if signal_id not in primary_signals_new:
            payload.append({"id": signal_id, cfg["update_field"]: []})

    if len(payload) == 0:
        return 0

    for record in payload:

        res = knackpy.record(
            record,
            obj_key=cfg["ref_obj"][0],
            app_id=knack_creds["app_id"],
            api_key=knack_creds["api_key"],
            method="update",
        )

    return len(payload)


if __name__ == "__main__":
    main()
