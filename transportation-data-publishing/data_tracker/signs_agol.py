# Publish pavement markings work orders to ArcGIS Online
import copy
import pdb
import traceback

import agolutil
import argutil
import arrow
import datautil
import emailutil
import knackutil
import knackpy

import _setpath
from config.secrets import *
from config.knack.config import SIGNS_AGOL as config



"""
get locations
get specs
merge them
get work orders
get attachments
get materials

"""

def parse_geometry(record, geometry_field_name):
    """
    Extract lat/lon from knack location record.
    """
    lat_field = f"{geometry_field_name}_latitude"
    lon_field = f"{geometry_field_name}_longitude"
    return record.get(lon_field), record.get(lat_field)


def process_locations(records, geometry_field_name, primary_key, wo_id_field="ATD_WORK_ORDER_ID"):
    """
    Construct a dictionary of select location attributes which will
    be joined to spec actual records.

    Parameters
    ----------
    records : list (required)
        The source location records retrieved from Knack.
    geometry_field_name : string (required)
        The field name in the location records that contains the location data. 
    primary_key : string (required)
        The field which uniquely identifies each location record.
    wo_id_field : string (required)
        The field which identifies the parent work order record.

    Returns
    -------
    dict of location record dicts

    """
    locations = {}

    for record in records:
        location = {}
        id_ = record.get(primary_key)
        wo_id = record.get(wo_id_field)
        x, y = parse_geometry(record, geometry_field_name)
        locations[id_] = {wo_id_field: wo_id, "x" : x, "y" : y}

    pdb.set_trace()
    return locations


def filter_by_date(data, date_field, compare_date):
    """Summary
    
    Args:
        data (TYPE): Description
        date_field (TYPE): Description
        compare_date (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return [record for record in data if record[date_field] >= compare_date]


def knackpy_wrapper(cfg, auth, obj=None, filters=None):
    """Summary
    
    Args:
        cfg (TYPE): Description
        auth (TYPE): Description
        obj (None, optional): Description
        filters (None, optional): Description
    
    Returns:
        TYPE: Description
    """
    return knackpy.Knack(
        obj=obj,
        scene=cfg["scene"],
        view=cfg["view"],
        ref_obj=cfg["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        page_limit=1,
        rows_per_page=10
    )


def cli_args():

    parser = argutil.get_parser(
        "signs_agol.py",
        "Publish Signs Work Order Data to ArcGIS Online",
        "app_name",
        "--replace",
        "--last_run_date",
    )

    args = parser.parse_args()

    return args


def main():

    args = cli_args()

    auth = KNACK_CREDENTIALS[args.app_name]

    records_processed = 0

    last_run_date = args.last_run_date

    if not last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        # the arrow package needs a specific date and timeformat
        last_run_date = "1970-01-01"
    """
    We include a filter in our API call to limit to records which have
    been modified on or after the date the last time this job ran
    successfully. The Knack API supports filter requests by date only
    (not time), so we must apply an additional filter on the data after
    we receive it.
    """
    for cfg in config:
        
        print(cfg["name"])

        filters = knackutil.date_filter_on_or_after(
            last_run_date, cfg["modified_date_field_id"]
        )

        kn = knackpy_wrapper(cfg, auth, filters=filters)

        if kn.data:
            # Filter data for records that have been modifed after the last
            # job run (see comment above)
            last_run_timestamp = arrow.get(last_run_date).timestamp * 1000
            kn.data = filter_by_date(
                kn.data, cfg["modified_date_field"], last_run_timestamp
            )

        if not kn.data:
            records_processed += 0
            continue

        records = kn.data

        if cfg["name"] == "work_order_signs_locations":
            # location data from this object is merged to asset spec records
            process_locations(kn.data, cfg["geometry_field_name"], cfg["primary_key"])
            print("hi")

        if cfg.get("extract_attachment_url"):
            records = knackutil.attachment_url(
                records, in_fieldname="ATTACHMENT", out_fieldname="ATTACHMENT_URL"
            )

        records = remove_empty_strings(
            records
        )  # AGOL has unexepected handling of empty values

        # update_layer = agolutil.get_item(
        #     auth=AGOL_CREDENTIALS,
        #     service_id=cfg["service_id"],
        #     layer_id=cfg["layer_id"],
        #     item_type=cfg["item_type"],
        # )

        # if args.replace:
        #     res = update_layer.delete_features(where="1=1")
        #     agolutil.handle_response(res)

        # else:
        #     """
        #     Delete objects by primary key. ArcGIS api does not currently support
        #     an upsert method, although the Python api defines one via the
        #     layer.append method, it is apparently still under development. So our
        #     "upsert" consists of a delete by primary key then add.
        #     """
        #     primary_key = cfg.get("primary_key")

        #     delete_ids = [record.get(primary_key) for record in records]
        #     delete_ids = ", ".join(f"'{x}'" for x in delete_ids)

        #     #  generate a SQL-like where statement to identify records for deletion
        #     where = "{} in ({})".format(primary_key, delete_ids)
        #     res = update_layer.delete_features(where=where)
        #     agolutil.handle_response(res)

        # for i in range(0, len(records), 1000):
        #     # insert agol features in chunks
        #     adds = agolutil.feature_collection(
        #         records[i : i + 1000], spatial_ref=102739
        #     )
        #     res = update_layer.edit_features(adds=adds)
        #     agolutil.handle_response(res)
        #     records_processed += len(adds)

    return records_processed


if __name__ == "__main__":
    main()