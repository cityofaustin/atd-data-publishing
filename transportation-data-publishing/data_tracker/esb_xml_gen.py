"""
Generate XML message to update 311 Service Reqeusts
via Enterprise Service Bus
"""
import os
import pdb

import arrow
import knackpy

import _setpath
from config.esb.config import cfg as CONFIG
from config.secrets import *

import argutil
import datautil


def encode_special_characters(text, lookup):
    """
    ESB requires ASCII characters only.
    We drop non-ASCII characters by encoding as ASCII with "ignore" flag
    """
    text = text.encode("ascii", errors="ignore")
    text = text.decode("ascii")

    #  We also encode invalid XML characters
    for char in lookup.keys():
        text = text.replace(char, lookup[char])

    return text


def get_csr_filters(emi_field, esb_status_field, esb_status_match):
    #  construct a knack filter object
    filters = {
        "match": "and",
        "rules": [
            {"field": emi_field, "operator": "is not blank"},
            {"field": esb_status_field, "operator": "is", "value": esb_status_match},
        ],
    }

    return filters


def check_for_data(app_name, cfg):
    #  check for data at public endpoint
    #  this api call does not count against
    #  daily subscription limit because we do not
    #  provide reference objects
    kn = knackpy.Knack(
        view=cfg["view"],
        scene=cfg["scene"],
        app_id=KNACK_CREDENTIALS[app_name]["app_id"],
        api_key="knack",
        page_limit=1,
        rows_per_page=1,
    )

    if kn.data_raw:
        return True
    else:
        return False


def get_data(app_name, cfg):
    """
    get data at public enpoint and also get
    necessary field metadata (which is not public)
    field data is fetched because we provide a ref_obj array
    """
    return knackpy.Knack(
        ref_obj=cfg["ref_obj"],
        view=cfg["view"],
        scene=cfg["scene"],
        app_id=KNACK_CREDENTIALS[app_name]["app_id"],
        api_key=KNACK_CREDENTIALS[app_name]["api_key"],
    )


def build_xml_payload(record, lookup, cfg):
    record[cfg["activity_details_fieldname"]] = format_activity_details(
        record, cfg["activity_name_fieldname"], cfg["activity_details_fieldname"]
    )
    record[cfg["activity_details_fieldname"]] = encode_special_characters(
        record[cfg["activity_details_fieldname"]], lookup
    )

    record["PUBLICATION_DATETIME"] = arrow.now().format()

    with open(cfg["template"], "r") as fin:
        template = fin.read()
        return template.format(**record)


def format_activity_details(record, activty_field_name, activity_details_field_name):
    activity = record[activty_field_name]
    details = record[activity_details_field_name]

    if activity and details:
        return "{} - {}".format(activity, details)
    elif activity or details:
        return "{}{}".format(activity, details)
    else:
        return ""


def set_workdir():
    """
    Set the working directory to the location of this script to
    ensure file outputs go to their intended places when
    script is run by an external file (e.g., the launcher)
    """
    path = os.path.dirname(__file__)

    if path:
        os.chdir(path)


def cli_args():

    parser = argutil.get_parser(
        "esb_xml_gen.py",
        "Generate XML message to update 311 Service Reqeusts via Enterprise Service Bus.",
        "app_name",
    )

    args = parser.parse_args()

    return args


def main():

    set_workdir()

    args = cli_args()

    app_name = args.app_name

    if "data_tracker" in app_name:
        source = "tmc_activities"
    elif "signs" in app_name:
        source = "signs_markings_activities"

    cfg = CONFIG[source]

    #  invalid XLM characters to be encoded
    SPECIAL_CHAR_LOOKUP = {
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&apos;",
        "&": "&amp;",
    }

    outpath = "{}/{}".format(ESB_XML_DIRECTORY, "ready_to_send")

    if not os.path.exists(outpath):
        os.makedirs(outpath)

    knack_creds = KNACK_CREDENTIALS

    #  check for data at public endpoint
    data = check_for_data(app_name, cfg)

    if data:
        #  get data at private enpoint
        kn = get_data(app_name, cfg)

    # should I just abort in this case?
    else:
        # logger.info('No new records to process')
        return 0

    #  identify date fields for conversion from mills to unix
    date_fields_kn = [
        kn.fields[f]["label"]
        for f in kn.fields
        if kn.fields[f]["type"] in ["date_time", "date"]
    ]

    kn.data = datautil.mills_to_iso(kn.data, date_fields_kn)

    for record in kn.data:
        payload = build_xml_payload(record, SPECIAL_CHAR_LOOKUP, cfg)
        """
        XML messages are formatted with incremental ATD_ACTIVITY_ID as well as
        database record id. 

        If for some reason this record already has an XML message in queue
        (e.g. the ESB is down), the previous message will be overwritten
        don't change the message format without considering esb_xml_send.py
        """
        with open(
            "{}/{}_-_{}_-_{}.xml".format(
                outpath, app_name, record[cfg["activity_id_field"]], record["id"]
            ),
            "w",
        ) as fout:
            fout.write(payload)

    return len(kn.data)


if __name__ == "__main__":
    main()
