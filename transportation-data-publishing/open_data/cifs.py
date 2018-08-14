import csv
import io
import json
import logging
import os
import pdb
import traceback

import arrow

import _setpath
from config.secrets import *
from tdutils import agolutil
from tdutils import emailutil
from tdutils import socratautil


socrata_creds = SOCRATA_CREDENTIALS
agol_creds = AGOL_CREDENTIALS


"""
TODO:
- geocoding
- actually just upsert on the tabular file>>it will contain historical closures
- replace json with empty file when there are no closures?
"""
now = arrow.now()
now_esri_query = now.format("YYYY-MM-DD HH:mm:ss")
now_mills = now.timestamp * 1000

script = os.path.basename(__file__).replace(".py", ".log")
logfile = f"{LOG_DIRECTORY}/{script}"
logger.basicConfig(filename=logfile, level=logger.INFO)
logger.info("START AT {}".format(now.format()))

socrata_resource_id_json = "fwsr-gb9r"
socrata_resource_id_csv = "aki2-nu5c"

agol_config = {
    "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/ATD_road_closures_incidents/FeatureServer/0/",
    "query_params": {
        "f": "json",
        "where": "EVENT_START_DATETIME < '{}' AND '{}' < EVENT_END_DATETIME".format(
            now_esri_query, now_esri_query
        ),
        # 'time' : now_mills,      per ESRI time query is defective on hosted featre layers
        "outFields": "*",
        "returnGeometry": True,
    },
}

reference_id = "1501"  # arbitrary org reference
reference_name = "City of Austin"

fieldmap = {
    "GlobalID": {
        "type": "string",
        "cifs_name": "-id",
        "required": True,
        "note": "as id valiue on incident tag",
    },
    "PRIMARY_STREET": {"type": "string", "cifs_name": "street", "required": True},
    "EVENT_TYPE": {"type": "string", "cifs_name": "type", "required": True},
    "DIRECTIONS_AFFECTED": {
        "type": "string",
        "cifs_name": "direction",
        "required": True,
    },
    "DESCRIPTION": {"type": "string", "cifs_name": "description", "required": True},
    "EVENT_START_DATETIME": {
        "type": "datetime",
        "cifs_name": "startime",
        "required": True,
    },
    "EVENT_END_DATETIME": {
        "type": "datetime",
        "cifs_name": "endtime",
        "required": True,
    },
    "CREATED_DATE": {"type": "datetime", "cifs_name": "creationtime", "required": True},
    "MODIFIED_DATE": {"type": "datetime", "cifs_name": "updatetime", "required": True},
}


def convertToIso(mills):
    return (
        arrow.get(mills / 1000).to("US/Central").format("YYYY-MM-DDTHH:mm:ssZZ")
    )  #  iso-8601


def mapfields(feature, fieldmap):
    new_feature = {}
    for f in feature:
        if f in fieldmap:
            if fieldmap[f]["type"] == "datetime":
                new_feature[fieldmap[f]["cifs_name"]] = convertToIso(feature[f])
            else:
                new_feature[fieldmap[f]["cifs_name"]] = feature[f]
    return new_feature


def buildPolyline(feature):
    paths = feature["geometry"]["paths"]

    if len(paths) > 1:
        raise Exception  #  more than one line path may be bad - need to examine geom of multipoint

    polyline = []

    for path in paths:
        for point in path:
            for coord in point:
                polyline.append(coord)

    return " ".join(str(coord) for coord in polyline)


def convertToTabular(incidents):
    rows = []

    for feature in incidents:
        row = {}
        for field in feature.keys():
            if field == "-id":  #  '-id' if used for CIFS per Waze JSON example
                feature["id"] = feature.pop(field)
                field = "id"
            if type(feature[field]) == dict:  #  flatten nested json fields
                for subfield in feature[field].keys():
                    row[subfield] = feature[field][subfield]
            else:
                row[field] = feature[field]
        rows.append(row)

    return rows


def main():
    incidents = []

    try:
        #  get closure data
        agol_config["query_params"]["token"] = agolutil.get_token(agol_creds)
        data = agolutil.query_layer(
            agol_config["service_url"], agol_config["query_params"]
        )

        #  translate closure data to CIF spec
        if "features" in data:
            print(data)
            for feature in data["features"]:
                incident = mapfields(feature["attributes"], fieldmap)
                incident["polyline"] = buildPolyline(feature)
                location = {
                    "street": incident.pop("street"),
                    "polyline": incident.pop("polyline"),
                }

                incident["location"] = location

                incident["source"] = {"reference": reference_id, "name": reference_name}
                incidents.append(incident)

        #  write to socrata json feed
        file_string = io.StringIO(json.dumps(incidents))
        upload_response = socratautil.replace_non_data_file(
            socrata_creds,
            socrata_resource_id_json,
            "traffic-incidents.json",
            file_string,
        )

        #  write to socrata tabular dataset
        tabular_data = convertToTabular(incidents)
        #  replace_response = socratautil.replace_data(socrata_creds, socrata_resource_id_csv, tabular_data)
        upsert_response = socratautil.upsert_data(
            socrata_creds, tabular_data, socrata_resource_id_csv
        )

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(error_text)
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            "CIFS Publication Failure",
            error_text,
            EMAIL["user"],
            EMAIL["password"],
        )


main()

logger.info("END AT: {}".format(arrow.now().format()))


# def build_incident(feature, fieldmap):
