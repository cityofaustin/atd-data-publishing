"""
Calculate # of business days elapsed and update records accordingly.

Developed specifically for measuring Traffic Control Plan (TCP) permit
application reviews in the Right-of-Way Management division. 
"""
import argparse
from datetime import datetime
import os
import pdb
import traceback

import knackpy
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

import _setpath
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

# define config variables
scene = "scene_754"
view = "view_1987"
obj = "object_147"
start_key = "SUBMITTED_DATE"
end_key = "REVIEW_COMPLETED_DATE"
elapsed_key = "DAYS_ELAPSED"

update_fields = ["DAYS_ELAPSED", "id"]


def get_calendar():
    return CustomBusinessDay(calendar=USFederalHolidayCalendar())


def handle_records(data, start_key, end_key, elapsed_key, calendar):
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
    index = pd.DatetimeIndex(start=start, end=end, freq=calendar)
    elapsed = len(index) - 1
    return elapsed


def cli_args():
    """
    Parse command-line arguments using argparse module.
    """
    parser = argutil.get_parser(
        "tcp_business_days.py",
        "Calculate # of business days elapsed and update records accordingly.",
        "app_name",
    )

    args = parser.parse_args()

    return args


def update_record(record, obj_key, creds):

    res = knackpy.record(
        record,
        obj_key=obj_key,
        app_id=creds["app_id"],
        api_key=creds["api_key"],
        method="update",
    )

    return res


def main(job, **kwargs):

    app_name = kwargs["app_name"]

    creds = KNACK_CREDENTIALS[app_name]

    kn = knackpy.Knack(
        scene=scene,
        view=view,
        ref_obj=[obj],
        app_id=creds["app_id"],
        api_key=creds["api_key"],
    )

    calendar = get_calendar()

    kn.data = handle_records(kn.data, start_key, end_key, elapsed_key, calendar)

    # logger.info( '{} Records to Update'.format(len(kn.data) ))

    if kn.data:
        kn.data = datautil.reduce_to_keys(kn.data, update_fields)
        kn.data = datautil.replace_keys(kn.data, kn.field_map)

        for i, record in enumerate(kn.data):
            print("Update record {} of {}".format(i, len(kn.data)))
            update_record(record, obj, creds)

    return len(kn.data)


if __name__ == "__main__":
    # script_name = os.path.basename(__file__).replace('.py', '')
    # logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    #
    # logger = logutil.timed_rotating_log(logfile)
    # logger.info('START AT {}'.format( datetime.today() ))

    try:
        args = cli_args()
        logger.info("args: {}".format(str(args)))

        app_name = args.app_name
        knack_creds = KNACK_CREDENTIALS[app_name]

        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source="knack",
            destination="knack",
            auth=JOB_DB_API_TOKEN,
        )

        job.start()

        results = main(knack_creds)

        job.result("success", records_processed=results)

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(error_text)

        email_subject = "Days Elapsed Update Failure"

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            email_subject,
            error_text,
            EMAIL["user"],
            EMAIL["password"],
        )

        job.result("error", message=str(e))

        raise e
