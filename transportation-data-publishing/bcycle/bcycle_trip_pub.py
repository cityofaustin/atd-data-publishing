# Check for new b-cycle data in Dropbox share and upload to
# Open Data portal (Socrata)

# B-Cycle staff put new trip data in Dropbox share on a monthly basis.

# Attributes:
#     resource_id (str): Description

import csv
import os
import pdb

import arrow
import dropbox
import requests

import _setpath
from config.secrets import *
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil
from tdutils import socratautil


# define config variables
resource_id = "tyfh-5r8s"


def max_date_socrata(resource_id):
    """
    Get the most recent trip date from socrata
    
    Args:
        resource_id (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    url = "https://data.austintexas.gov/resource/{}.json?$query=SELECT checkout_date as date ORDER BY checkout_date DESC LIMIT 1".format(
        resource_id
    )
    res = requests.get(url)
    res.raise_for_status()
    return res.json()[0]["date"]


def get_data(path, token):
    """
    Get trip data file as string from dropbox
    
    Args:
        path (TYPE): Description
        token (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    # logger.info(f"Get data for {path}")

    dbx = dropbox.Dropbox(token)

    metadata, res = dbx.files_download(path)
    res.raise_for_status()

    return res.text


def handle_data(data):
    """
    Convert data file string to csv dict. Source column headers are replaced
    with database-friendly field names
    
    Args:
        data (TYPE): Description
    
    Returns:
        TYPE: Description
    """
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
    del (rows[0])  #  remove header row
    reader = csv.DictReader(rows, fieldnames)
    return list(reader)


def main():
    """
    Args:
        job
        **kwargs
    
    Returns:
    
    Raises:
        e: Description
    
    """

    dt_current = arrow.now().replace(months=-1)
    dt_current_formatted = dt_current.format("MM-YYYY")
    up_to_date = False
    results = None
    

    while not up_to_date:

        socrata_dt = max_date_socrata(resource_id)
        socrata_dt_formatted = arrow.get(socrata_dt).format("MM-YYYY")

        if dt_current_formatted == socrata_dt_formatted:
            # pdb.set_trace()
            up_to_date = True
            # job.result("success", records_processed=0)
            results = 0

        else:
            #  socrata data is at least one month old
            dropbox_month = arrow.get(socrata_dt).replace(months=1).format("MM")
            dropbox_year = arrow.get(socrata_dt).replace(months=1).format("YYYY")

            current_file = "TripReport-{}{}.csv".format(dropbox_month, dropbox_year)
            root = "austinbcycletripdata"  # note the lowercase-ness
            path = "/{}/{}/{}".format(root, dropbox_year, current_file)
            date_fields = ["checkout_date"]
            pdb.set_trace()    
            try:

                data = get_data(path, DROPBOX_BCYCLE_TOKEN)
                print(data)
                results = len(data)
                pdb.set_trace()

            except dropbox.exceptions.ApiError as e:

                if "LookupError" in str(e):
                    # end loop when no file can be found
                    # logger.warning(f"No data found for {path}")
                    up_to_date = True
                    # job.result("success")
                    break

                else:
                    # job.result("error", message=str(e))
                    raise e

                        
            data = handle_data(data)
            # logger.info("{} records found".format(len(data)))

            socratautil.Soda(
                auth=SOCRATA_CREDENTIALS,
                records=data,
                resource=resource_id,
                location_field=None,
            )

            # logger.info(res.json())

            # job.result("success", records_processed=len(data))

            results = len(data)

    return results

if __name__ == "__main__":
    main()
