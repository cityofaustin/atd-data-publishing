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


def sanitize_html(kn):
    """
    Replace html breaks from knack paragraph fields. Also, engineers like to put
    "<>" in their notes to indicate sign arrows. we have to replace these, too"
    """
    strs = ["<br />", "<", ">"]

    fields = [
        kn.fields[field]["label"]
        for field in kn.fields.keys()
        if kn.fields[field]["type"] == "paragraph_text"
    ]

    for record in kn.data:
        for field in fields:
            if record.get(field):
                for s in strs:
                    record[field] = record[field].replace(s, "")
    return kn


def append_locations_work_orders(config):
    """
    Append multiple work location points to each work order
    """
    work_orders = config["work_orders_signs"]["records"]
    spec_actuals = config["work_orders_signs_asset_spec_actuals"]["records"]
    join_field = config["work_orders_signs_asset_spec_actuals"]["work_order_id_field"]

    for wo in work_orders:
        geometries = []

        wo_id = wo.get(join_field)

        for sp in spec_actuals:
            if sp.get(join_field) == wo_id:
                x = sp.get("x")
                y = sp.get("y")
                if x and y:
                    geometries.append((x, y))

        # not that `points` key is required by arcgis geometry spec for multipoint features
        # https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
        wo["points"] = geometries

    return work_orders


def append_locations_to_specs(config, lon_field="x", lat_field="y"):
    """
    Append location attributes to spec actual records.

    Parameters
    ----------
    config : list (required)
        The external configuration object which defines layer parameters.
    lon_field : string (required)
        The field in the location records which contains the longitude value.
    lat_field : string (required)
        The field in the location records which contains the latitude value.

    Returns
    -------
    Configuration location attributes appended to spec actual records
    """

    records = config["work_orders_signs_asset_spec_actuals"].get("records")
    join_field = config["work_orders_signs_asset_spec_actuals"].get(
        "location_join_field"
    )
    work_order_id_field = config["work_orders_signs_asset_spec_actuals"].get(
        "work_order_id_field"
    )
    locations = config["work_order_signs_locations"].get("records")

    for record in records:
        location = locations.get(record[join_field])

        if not location:
            # TODO: better handling of missing location ids? for some reason the Knack view
            # does not properly filter to exclude specs with no location (which have been presumably
            # deleted without the spec being deleted as well)
            continue

        record[lon_field] = location[lon_field]
        record[lat_field] = location[lat_field]
        record[work_order_id_field] = location[work_order_id_field]

    # exclude records with missing geometry
    records = [rec for rec in records if rec.get("x")]

    return records


def parse_geometry(record, geometry_field_name):
    """
    Extract lat/lon from knack location record.
    """
    lat_field = f"{geometry_field_name}_latitude"
    lon_field = f"{geometry_field_name}_longitude"
    return record.get(lon_field), record.get(lat_field)


def process_locations(
    records, geometry_field_name, primary_key, wo_id_field="ATD_WORK_ORDER_ID"
):
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
        locations[id_] = {wo_id_field: wo_id, "x": x, "y": y}

    return locations


def fetch_records(cfg, last_run_date, auth):

    filters = knackutil.date_filter_on_or_after(
        last_run_date, cfg["modified_date_field_id"]
    )

    kn = knackpy_wrapper(cfg, auth, filters=filters)

    if kn.data:

        kn = sanitize_html(kn)

        # Filter data for records that have been modifed after the last
        # job run (see comment above)
        last_run_timestamp = arrow.get(last_run_date).timestamp * 1000

        kn.data = filter_by_date(
            kn.data, cfg["modified_date_field"], last_run_timestamp
        )

    return kn.data


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

    return knackpy.Knack(
        obj=obj,
        scene=cfg["scene"],
        view=cfg["view"],
        ref_obj=cfg["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        page_limit=100000,
        rows_per_page=1000,
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
    global config

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
    for cfg in config.items():
        # fetch data for all config objects
        cfg[1]["records"] = fetch_records(cfg[1], last_run_date, auth)

    config["work_order_signs_locations"]["records"] = process_locations(
        config["work_order_signs_locations"]["records"],
        config["work_order_signs_locations"]["geometry_field_name"],
        config["work_order_signs_locations"]["primary_key"],
    )

    config["work_orders_signs_asset_spec_actuals"][
        "records"
    ] = append_locations_to_specs(config)

    config["work_orders_signs"]["records"] = append_locations_work_orders(config)

    # drop work orders with no locations
    config["work_orders_signs"]["records"] = [
        x for x in config["work_orders_signs"]["records"] if x.get("points")
    ]

    # extract attachment url from each attachment record
    for record in config["work_orders_attachments"]["records"]:
        if record.get("ATTACHMENT"):
            record["ATTACHMENT_URL"] = record.get("ATTACHMENT")
            record.pop("ATTACHMENT")

    for name, cfg in config.items():

        if not cfg.get("service_id"):
            # ignore confige objects that do not hav service ids, i.e., do not
            # have agol acontent, i.e., the locations object which was merged into
            # other layers
            continue

        update_layer = agolutil.get_item(
            auth=AGOL_CREDENTIALS,
            service_id=cfg["service_id"],
            layer_id=cfg["layer_id"],
            item_type=cfg["item_type"],
        )

        if args.replace:
            res = update_layer.delete_features(where="1=1")
            agolutil.handle_response(res)

        else:
            """
            Delete objects by primary key in chunks. ArcGIS api does not currently support
            an upsert method, although the Python api defines one via the
            layer.append method, it is apparently still under development. So our
            "upsert" consists of a delete by primary key then add.
            """
            primary_key = cfg.get("primary_key")

            for i in range(0, len(cfg["records"]), 1000):

                delete_ids = [
                    record.get(primary_key) for record in cfg["records"][i : i + 1000]
                ]
                delete_ids = ", ".join(f"'{x}'" for x in delete_ids)

                #  generate a SQL-like where statement to identify records for deletion
                where = "{} in ({})".format(primary_key, delete_ids)
                res = update_layer.delete_features(where=where)

                agolutil.handle_response(res)

        for i in range(0, len(cfg["records"]), 1000):
            # insert agol features in chunks

            # assemble an arcgis feature collection set from records
            records = agolutil.feature_collection(
                cfg["records"][i : i + 1000], lat_field="y", lon_field="x"
            )

            # insert new features
            res = update_layer.edit_features(adds=records)

            agolutil.handle_response(res)

            records_processed += len(records)

    return records_processed


if __name__ == "__main__":
    main()
