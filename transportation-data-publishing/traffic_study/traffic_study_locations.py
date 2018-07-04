"""
Download traffic study locations form ArcGIS Online
and publish to Austin's Open Data Portal
"""
import csv
import os
import pdb
import traceback

import arrow

import _setpath
from config.secrets import *
from tdutils import agolutil
from tdutils import emailutil
from tdutils import logutil
from tdutils import jobutil
from tdutils import socratautil


SOCRATA_RESOURCE_ID = "jqhg-imb3"

FIELDNAMES = [
    "COMMENT_FIELD2",
    "START_DATE",
    "SITE_CODE",
    "COMMENT_FIELD1",
    "GLOBALID",
    "DATA_FILE",
    "COMMENT_FIELD4",
    "COMMENT_FIELD3",
    "LATITUDE",
    "LONGITUDE",
]

CONFIG = {
    "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/Traffic_Count_Location/FeatureServer/0/",
    "service_id": "3c56025e645045998ee499c0725dfebb",
    "params": {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": True,
        "outSr": 4326,  #  return WGS84
    },
}


def parse_mills(d):
    dt = arrow.get(d / 1000)
    tz = "US/Central"
    dt = dt.replace(tzinfo=tz)
    utc = dt.to("utc").isoformat()
    return utc


def main():

    layer = agolutil.get_item(auth=AGOL_CREDENTIALS, service_id=CONFIG["service_id"])

    features = layer.query(**CONFIG["params"])

    features_add = []

    for feature in features:
        feature_add = {
            key.upper(): value
            for key, value in feature.attributes.items()
            if key.upper() in FIELDNAMES
        }
        feature_add["LONGITUDE"] = float(
            str(feature.geometry["x"])[:10]
        )  #  truncate coordinate
        feature_add["LATITUDE"] = float(str(feature.geometry["y"])[:10])

        if feature_add.get("START_DATE"):
            feature_add["START_DATE"] = parse_mills(feature_add["START_DATE"])

        features_add.append(feature_add)

    socratautil.Soda(
        auth=SOCRATA_CREDENTIALS,
        resource=SOCRATA_RESOURCE_ID,
        records=features_add,
        lat_field="LATITUDE",
        lon_field="LONGITUDE",
        location_field="location",
        replace=True,
    )

    return len(features_add)


if __name__ == "__main__":

    script_name = os.path.basename(__file__).replace(".py", "")
    logfile = f"{LOG_DIRECTORY}/{script_name}.log"
    logger = logutil.timed_rotating_log(logfile)

    logger.info("START AT {}".format(arrow.now()))

    try:
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source="agol",
            destination="socrata",
            auth=JOB_DB_API_TOKEN,
        )

        job.start()

        results = main()

        job.result("success", records_processed=results)

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(error_text)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            "Traffic Count Locations Process Failure",
            error_text,
            EMAIL["user"],
            EMAIL["password"],
        )

        job.result("error", message=error_text)

        raise e

    logger.info("END AT: {}".format(arrow.now()))
