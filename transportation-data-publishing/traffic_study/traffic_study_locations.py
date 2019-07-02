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
import agolutil
import emailutil
import logutil
import jobutil
import socratautil


def parse_mills(d):
    dt = arrow.get(d / 1000)
    tz = "US/Central"
    dt = dt.replace(tzinfo=tz)
    utc = dt.to("utc").isoformat()
    return utc


def main():
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
        lat_field="latitude",
        lon_field="longitude",
        location_field="location",
        replace=True,
    )

    return len(features_add)


if __name__ == "__main__":
    results = main()
