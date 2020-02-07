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
from config.knack.config import MARKINGS_AGOL as config


# we use this global to store wo geometries so they can be accessed
# when fetching geometries for jobs. it aint pretty, but is it really so bad?
work_order_geometries = None


def get_paths_from_work_orders(
    records, match_field="ATD_WORK_ORDER_ID", output_field="paths"
):
    # copy work order geometries from work order to child jobs
    for record in records:
        for wo in work_order_geometries:
            if record[match_field] == wo[match_field]:
                record[output_field] = wo[output_field]
                continue

    return records


def remove_empty_strings(records):
    new_records = []
    for record in records:
        new_record = {
            key: record[key]
            for key in record.keys()
            if not (type(record[key]) == str and not record[key])
        }
        new_records.append(new_record)
    return new_records


def remove_empty_strings(records):
    new_records = []
    for record in records:
        new_record = {
            key: record[key]
            for key in record.keys()
            if not (type(record[key]) == str and not record[key])
        }
        new_records.append(new_record)
    return new_records


def append_paths(
    records,
    features,
    multi_source_geometry=False,
    path_id_field=None,
    output_field="paths",
):
    """Append path geometries from a esri polyline data source to input records.

    Input records are assumed to contain either a list stored at 'path_id_field' which
    contains an array of ids matching the input spatial features, or a singular string
    stored as 'path_id_field' which uniquely identfies a feature in the source geomtery. 
    """
    unmatched = ""

    for record in records:

        path_id = record.get(path_id_field)

        if path_id:
            paths = []

            if type(path_id) == str:
                for feature in features:
                    if path_id == feature.attributes.get(path_id_field):
                        try:
                            paths = [path for path in feature.geometry["paths"]]
                        except TypeError:
                            pass

                        record[output_field] = paths
                        break

            elif type(path_id) == list:
                for path_id in record[path_id_field]:
                    for feature in features:
                        if str(path_id) == str(feature.attributes.get(path_id_field)):
                            paths = paths + [path for path in feature.geometry["paths"]]

                record[output_field] = paths

            if not record.get(output_field):
                unmatched += f"{path_id_field}: {path_id}\n"

    if unmatched:
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            f"Markings AGOL: Geomtries Not Found",
            unmatched,
            EMAIL["user"],
            EMAIL["password"],
        )

    return records


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
        page_limit=1000000,
    )


def cli_args():

    parser = argutil.get_parser(
        "markings_agol.py",
        "Publish Signs and Markings Work Order Data to ArcGIS Online",
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

        if cfg.get("name") == "markings_work_orders":
            #  markings work order geometries are retrieved from AGOL

            # reduce to unique segment ids from all records
            segment_ids = datautil.unique_from_list_field(
                records, list_field=cfg["geometry_record_id_field"]
            )

            if segment_ids:

                geometry_layer = agolutil.get_item(
                    auth=AGOL_CREDENTIALS,
                    service_id=cfg["geometry_service_id"],
                    layer_id=cfg["geometry_layer_id"],
                )

                source_geometries_all = []

                chunksize = 200

                for i in range(0, len(segment_ids), chunksize):
                    # fetch agol source geometries in chunks
                    where_ids = ", ".join(
                        f"'{x}'" for x in segment_ids[i : i + chunksize]
                    )

                    if where_ids:
                        where = "{} in ({})".format(
                            cfg["geometry_record_id_field"], where_ids
                        )

                        source_geometries_chunk = geometry_layer.query(
                            where=where, outFields=cfg["geometry_record_id_field"]
                        )

                        if not source_geometries_chunk:
                            raise Exception(
                                "No features returned from source geometry layer query"
                            )

                        source_geometries_all.extend(source_geometries_chunk)

                records = append_paths(
                    kn.data,
                    source_geometries_all,
                    path_id_field=cfg["geometry_record_id_field"],
                )

                global work_order_geometries
                work_order_geometries = copy.deepcopy(records)

        elif cfg.get("name") == "markings_jobs":
            # get data from markings records
            records = get_paths_from_work_orders(records)

        if cfg.get("extract_attachment_url"):
            for record in records:
                if record.get("ATTACHMENT"):
                    record["ATTACHMENT_URL"] = record.get("ATTACHMENT")
                    record.pop("ATTACHMENT")

        records = remove_empty_strings(
            records
        )  # AGOL has unexepected handling of empty values

        update_layer = agolutil.get_item(
            auth=AGOL_CREDENTIALS,
            service_id=cfg["service_id"],
            layer_id=cfg["layer_id"],
            item_type=cfg["item_type"],
        )

        if args.replace:
            # we used to delete all features using a `where="1=1"` statement, but fails with a large number
            # of features. so we now fetch  the OIDs of existing features, and pass them to the delete
            existing_features = update_layer.query(
                return_geometry=False, out_fields="OBJECTID"
            )
            oids = [
                str(f.attributes.get("OBJECTID")) for f in existing_features.features
            ]

            if oids:
                oid_chunksize = 500
                for i in range(0, len(oids), oid_chunksize):
                    # we delete in chunks because Esri doesn't like deleting lots of features at once
                    deletes = ", ".join(oids[i : i + oid_chunksize])
                    res = update_layer.delete_features(deletes=deletes)
                    agolutil.handle_response(res)

        else:
            """
            Delete objects by primary key. ArcGIS api does not currently support
            an upsert method, although the Python api defines one via the
            layer.append method, it is apparently still under development. So our
            "upsert" consists of a delete by primary key then add.
            """
            primary_key = cfg.get("primary_key")

            delete_ids = [record.get(primary_key) for record in records]
            delete_ids = ", ".join(f"'{x}'" for x in delete_ids)

            #  generate a SQL-like where statement to identify records for deletion
            where = "{} in ({})".format(primary_key, delete_ids)
            res = update_layer.delete_features(where=where)
            agolutil.handle_response(res)

        for i in range(0, len(records), 1000):
            # insert agol features in chunks
            adds = agolutil.feature_collection(
                records[i : i + 1000], spatial_ref=102739
            )
            res = update_layer.edit_features(adds=adds)
            agolutil.handle_response(res)
            records_processed += len(adds)

    return records_processed


if __name__ == "__main__":
    main()
