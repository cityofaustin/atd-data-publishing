# Calculate # of business days elapsed and update records accordingly.

# Developed specifically for measuring Traffic Control Plan (TCP) permit
# application reviews in the Right-of-Way Management division.

from datetime import datetime

import knackpy
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import argutil
import datautil

import _setpath
from config.secrets import *
from config.knack.config import TCP_BUSINESS_DAYS as config


def get_calendar():
    """Summary
    
    Returns:
        TYPE: Description
    """
    return CustomBusinessDay(calendar=USFederalHolidayCalendar())


def handle_records(data, start_key, end_key, elapsed_key, calendar):
    """Summary
    
    Args:
        data (TYPE): Description
        start_key (TYPE): Description
        end_key (TYPE): Description
        elapsed_key (TYPE): Description
        calendar (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    update = []

    for record in data:
        dates = get_dates(record, start_key, end_key)

        if dates:
            elapsed = business_days_elapsed(dates["start"], dates["end"], calendar)

            try:
                old_elapsed = int(record.get(elapsed_key))

            except ValueError:
                #  assume old_elapsed is an empty string
                record[elapsed_key] = int(elapsed)
                update.append(record)
                continue

            if int(record[elapsed_key]) != int(elapsed):
                record[elapsed_key] = int(elapsed)
                update.append(record)

            else:
                continue
        else:
            continue

    return update


def get_dates(record, start_key, end_key):
    """Summary
    
    Args:
        record (TYPE): Description
        start_key (TYPE): Description
        end_key (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    start = record.get(start_key)

    if start:
        start = datetime.fromtimestamp(int(start) / 1000)
    else:
        return None

    end = record.get(end_key)

    if end:
        end = datetime.fromtimestamp(int(end) / 1000)
    else:
        end = datetime.today()

    return {"start": start, "end": end}


def business_days_elapsed(start, end, calendar):
    """Summary
    
    Args:
        start (TYPE): Description
        end (TYPE): Description
        calendar (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    date_range = pd.date_range(start=start, end=end, freq=calendar)
    index = pd.DatetimeIndex(date_range)
    elapsed = len(index) - 1
    return elapsed


def cli_args():
    parser = argutil.get_parser(
        "tcp_business_days.py",
        "Calculate # of business days elapsed and update records accordingly.",
        "app_name",
    )

    args = parser.parse_args()

    return args


def update_record(record, obj_key, creds):
    """Summary
    
    Args:
        record (TYPE): Description
        obj_key (TYPE): Description
        creds (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    res = knackpy.record(
        record,
        obj_key=obj_key,
        app_id=creds["app_id"],
        api_key=creds["api_key"],
        method="update",
    )

    return res


def main():

    args = cli_args()
    app_name = args.app_name

    creds = KNACK_CREDENTIALS[app_name]

    kn = knackpy.Knack(
        scene=config["scene"],
        view=config["view"],
        ref_obj=[config["obj"]],
        app_id=creds["app_id"],
        api_key=creds["api_key"],
    )

    calendar = get_calendar()

    kn.data = handle_records(
        kn.data, config["start_key"], config["end_key"], config["elapsed_key"], calendar
    )

    if kn.data:
        kn.data = datautil.reduce_to_keys(kn.data, config["update_fields"])
        kn.data = datautil.replace_keys(kn.data, kn.field_map)

        for i, record in enumerate(kn.data):
            print("Update record {} of {}".format(i, len(kn.data)))
            update_record(record, config["obj"], creds)
    return len(kn.data)


if __name__ == "__main__":
    main()
