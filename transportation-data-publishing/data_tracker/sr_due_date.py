"""
Update 311 SRs with their due date. The due date is stored on a related flex note record.
"""
from datetime import datetime
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
from config.knack.config import SR_DUE_DATE as cfg

import argutil


def sr_filter(sr_id, field_id):

    return {
        "match": "and",
        "rules": [
            {"field": f"field_1455", "operator": "is", "value": f"SRSLADAT"},
            {"field": f"{field_id}", "operator": "is", "value": f"{sr_id}"},
        ],
    }


def get_due_date(date):
    """ 
    Parse date and return as mm/dd/yyyy.

    Input format, e.g.: 'DEC 04, 2018'
    """
    date = date.title()
    date = datetime.strptime(date, "%b %d, %Y")
    return date.strftime("%m/%d/%Y")


def cli_args():
    parser = argutil.get_parser(
        "sr_due_date.py", "Update 311 SRs with their due date.","app_name"
    )

    args = parser.parse_args()

    return args


def main():

    args = cli_args()

    app_name = args.app_name

    srs = knackpy.Knack(
        view=cfg["tmc_issues"]["view"],
        scene=cfg["tmc_issues"]["scene"],
        ref_obj=cfg["tmc_issues"]["ref_obj"],
        app_id=KNACK_CREDENTIALS[app_name]["app_id"],
        api_key=KNACK_CREDENTIALS[app_name]["api_key"],
    )

    count = 0

    if not srs.data:
        return 0
        
    for sr in srs.data:

        filters = sr_filter(sr["SR_NUMBER"], cfg["flex_notes"]["sr_id_field"])

        flex_note = knackpy.Knack(
            view=cfg["flex_notes"]["view"],
            scene=cfg["flex_notes"]["scene"],
            ref_obj=cfg["flex_notes"]["ref_obj"],
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            filters=filters,
            page_limit=1,  # limit records, to be safe (there are lots)
            rows_per_page=10,
        )

        if not flex_note.data:
            continue

        """
        Always take the first due date in the list. there are occasionally duplicate
        due date flex records for one SR. We don't know why.
        """
        due_date = get_due_date(flex_note.data[0]["FLEX_ATTRIBUTE_VALUE"])

        record = {cfg["tmc_issues"]["due_date_field_id"]: due_date, "id": sr["id"]}

        res = knackpy.record(
            record,
            obj_key=cfg["tmc_issues"]["ref_obj"][0],
            app_id=KNACK_CREDENTIALS[app_name]["app_id"],
            api_key=KNACK_CREDENTIALS[app_name]["api_key"],
            method="update",
        )

        count +=1

    return count

if __name__ == "__main__":
    main()
