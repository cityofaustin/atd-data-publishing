"""
Check for new b-cycle data in Dropbox share and upload to Open Data portal (Socrata)
B-Cycle staff put new trip data in Dropbox share on a monthly basis.
"""

import csv
from datetime import datetime

import arrow
import dropbox
import requests

import _setpath
from config.secrets import *
import socratautil


# define config variables
resource_id = "tyfh-5r8s"


def get_newest_socata_record(resource_id):
    url = "https://data.austintexas.gov/resource/{}.json?$query=SELECT checkout_date as date ORDER BY checkout_date DESC LIMIT 1".format(
        resource_id
    )
    res = requests.get(url)
    res.raise_for_status()
    return res.json()[0]["date"]


def get_data(path, token):
    """Get trip data file as string from dropbox"""
    # logger.info(f"Get data for {path}")

    dbx = dropbox.Dropbox(token)

    try:
        metadata, res = dbx.files_download(path)

    except dropbox.exceptions.ApiError:
        raise Exception(
            f"No data available at {path}. Dropbox data may not be current."
        )

    res.raise_for_status()

    return res.text


def handle_data(data):
    """Convert data file string to csv dict. Source column headers are replaced
    with database-friendly field names"""
    #  assume fields in this order  :(

    fieldnames = (
        "trip_id",
        "membership_type",
        "bicycle_id",
        "checkout_date",
        "checkout_time",
        "checkout_kiosk_id",
        "checkout_kiosk",
        "return_kiosk_id",
        "return_kiosk",
        "trip_duration_minutes",
    )

    rows = data.splitlines()
    del rows[0]  # remove header row
    reader = csv.DictReader(rows, fieldnames)
    return list(reader)


def format_checkout_date(data, year, date_field="checkout_date"):
    # socrautil uses arrow to handle dates, but it no longer understands: mm/dd/yyyy
    # so convert to yyyy-mm-dd
    for row in data:
        month, day, two_digit_year = row[date_field].split("/")
        row[date_field] = arrow.get(f"{year}-{month}-{day}").format("YYYY-MM-DD")
    return data


def main():
    today = arrow.get(datetime.today())
    # bcycle dropbox data is only ever available for the previous month
    max_socrata_dt = today.shift(months=-1)
    # we want to test against the first day of the last month, so construct that
    results = 0

    while True:

        current_dt_socrata = arrow.get(get_newest_socata_record(resource_id))

        if current_dt_socrata >= max_socrata_dt:
            break

        # try to download the file for month for which we do not have data
        next_file_date = current_dt_socrata.shift(months=+1)
        dropbox_file_dt = next_file_date.format("MMYYYY")

        current_file = "TripReport-{}.csv".format(dropbox_file_dt)
        root = "austinbcycletripdata"  # note the lowercase-ness
        path = "/{}/{}/{}".format(root, next_file_date.year, current_file)

        try:
            data = get_data(path, DROPBOX_BCYCLE_TOKEN)
            results = len(data)
        except TypeError:
            results = 0
        except dropbox.exceptions.ApiError as e:

            if "LookupError" in str(e):
                # end loop when no file can be found
                break
            else:
                raise e

        data = handle_data(data)
        data = format_checkout_date(data, next_file_date.year)
        socratautil.Soda(
            auth=SOCRATA_CREDENTIALS,
            records=data,
            resource=resource_id,
            location_field=None,
            source="bcycle",
        )

        results += len(data)

    return results


if __name__ == "__main__":
    main()
