
# Get latest b-cycle kiosk data from Dropbox and upload to Socrata, ArcGIS Online

# Attributes:
#     dropbox_path (str): Description
#     service_id (str): Description
#     socrata_resource_id (str): Description

import csv
import os
import pdb

import arrow
import dropbox
import requests


import _setpath
from config.secrets import *
from tdutils import agolutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil
from tdutils import socratautil

service_id = "7d4d0b1369504383a42b943bd9c03f9a"
socrata_resource_id = "qd73-bsdg"
dropbox_path = "/austinbcycletripdata/kiosks.csv"


def get_data(path, token):
    """
    Get trip data file as string from dropbox
    
    Args:
        path (TYPE): Description
        token (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    # logger.info(f'Get data for {path}')

    dbx = dropbox.Dropbox(token)

    metadata, res = dbx.files_download(path)
    res.raise_for_status()

    return res.text


def handle_data(data):
    """
    Convert data file string to csv dict.
    
    Args:
        data (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    rows = data.splitlines()
    reader = csv.DictReader(rows)
    return list(reader)


def main():
    """Summary
    
    Args:
        jobs (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Descriptio
    """
    script_name = os.path.basename(__file__).replace(".py", "")

    # job_agol = jobutil.Job(
    #     name=f"{script_name}_agol",
    #     url=JOB_DB_API_URL,
    #     source="dropbox",
    #     destination="agol",
    #     auth=JOB_DB_API_TOKEN,
    # )

    # job_agol.start()

    # job_socrata = jobutil.Job(
    #     name=f"{script_name}_socrata",
    #     url=JOB_DB_API_URL,
    #     source="dropbox",
    #     destination="socrata",
    #     auth=JOB_DB_API_TOKEN,
    # )

    # job_socrata.start()

    data = get_data(dropbox_path, DROPBOX_BCYCLE_TOKEN)
    data = handle_data(data)
    data = datautil.upper_case_keys(data)

    data = datautil.replace_keys(data, {"STATUS": "KIOSK_STATUS"})

    layer = agolutil.get_item(auth=AGOL_CREDENTIALS, service_id=service_id)

    res = layer.manager.truncate()
    agolutil.handle_response(res)

    adds = agolutil.feature_collection(data)

    res = layer.edit_features(adds=adds)
    agolutil.handle_response(res)

    socratautil.Soda(
        auth=SOCRATA_CREDENTIALS,
        records=data,
        resource=socrata_resource_id,
        lat_field="latitude",
        lon_field="longitude",
        location_field="location",
        replace=True,
    )

    return len(data)


if __name__ == "__main__":
    main()
