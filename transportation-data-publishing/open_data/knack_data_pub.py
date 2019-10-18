"""
Extract data from Knack database and publish to Socrata, ArcGIS Online, 
or CSV.

By default, destination data is incrementally updated based on a
modified date field defined in the configuration file. Alternatively use
--replace to truncate/replace the entire dataset. CSV output is always handled
with --replace.

#TODO
- filter fetch locations by source data
"""
import arrow
import knackpy

import _setpath
from config.knack.config import cfg
from config.secrets import *

import agolutil
import argutil
import datautil
import knackutil
import socratautil


def socrata_pub(records, cfg_dataset, replace, date_fields=None):
    """Summary
    
    Args:
        records (TYPE): Description
        cfg_dataset (TYPE): Description
        replace (TYPE): Description
        date_fields (None, optional): Description
    
    Returns:
        TYPE: Description
    """
    if cfg_dataset.get("location_fields"):
        lat_field = cfg_dataset["location_fields"]["lat"].lower()
        lon_field = cfg_dataset["location_fields"]["lon"].lower()
        location_field = cfg_dataset["location_fields"]["location_field"].lower()
    else:
        lat_field = None
        lon_field = None
        location_field = None

    return socratautil.Soda(
        auth=SOCRATA_CREDENTIALS,
        records=records,
        resource=cfg_dataset["socrata_resource_id"],
        date_fields=date_fields,
        lat_field=lat_field,
        lon_field=lon_field,
        location_field=location_field,
        replace=replace,
    )


def agol_pub(records, cfg_dataset, replace):
    """
    Upsert or replace records on arcgis online features service
    
    Args:
        records (TYPE): Description
        cfg_dataset (TYPE): Description
        replace (TYPE): Description
    
    Returns:
        TYPE: Description
    
    Raises:
        Exception: Description
    """
    if cfg_dataset.get("location_fields"):
        lat_field = cfg_dataset["location_fields"]["lat"]
        lon_field = cfg_dataset["location_fields"]["lon"]
    else:
        lat_field = None
        lon_field = None

    layer = agolutil.get_item(
        auth=AGOL_CREDENTIALS, service_id=cfg_dataset["service_id"]
    )

    if replace:
        res = layer.manager.truncate()

        if not res.get("success"):
            raise Exception("AGOL truncate failed.")

    else:
        """
        Delete objects by primary key. ArcGIS api does not currently support
        an upsert method, although the Python api defines one via the
        layer.append method, it is apparently still under development. So our
        "upsert" consists of a delete by primary key then add.
        """
        primary_key = cfg_dataset.get("primary_key")

        delete_ids = [record[primary_key] for record in records]

        delete_ids = ", ".join(f"'{x}'" for x in delete_ids)

        #  generate a SQL-like where statement to identify records for deletion
        where = "{} in ({})".format(primary_key, delete_ids)
        res = layer.delete_features(where=where)
        agolutil.handle_response(res)

    for i in range(0, len(records), 1000):
        print(i)
        adds = agolutil.feature_collection(
            records[i : i + 1000], lat_field=lat_field, lon_field=lon_field
        )
        res = layer.edit_features(adds=adds)
        agolutil.handle_response(res)

    return True


def write_csv(knackpy_instance, cfg_dataset, dataset):
    """Summary
    
    Args:
        knackpy_instance (TYPE): Description
        cfg_dataset (TYPE): Description
        dataset (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    if cfg_dataset.get("csv_separator"):
        sep = cfg_dataset["csv_separator"]
    else:
        sep = ","

    file_name = "{}/{}.csv".format(FME_DIRECTORY, dataset)
    knackpy_instance.to_csv(file_name, delimiter=sep)

    return True


def knackpy_wrapper(cfg_dataset, auth, filters=None):
    """Summary
    
    Args:
        cfg_dataset (TYPE): Description
        auth (TYPE): Description
        filters (None, optional): Description
    
    Returns:
        TYPE: Description
    """
    return knackpy.Knack(
        obj=cfg_dataset["obj"],
        scene=cfg_dataset["scene"],
        view=cfg_dataset["view"],
        ref_obj=cfg_dataset["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        page_limit=10000,
    )


def get_multi_source(cfg_dataset, auth, last_run_date):
    """
    Return a single knackpy dataset instance from multiple knack sources. Developed
    specifically for merging traffic and phb signal requests into a single dataset.
    
    See note in main() about why we filter by date twice for each knackpy instance.
    
    Args:
        cfg_dataset (TYPE): Description
        auth (TYPE): Description
        last_run_date (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    kn = None

    for source_cfg_dataset in cfg_dataset["sources"]:

        filters = knackutil.date_filter_on_or_after(
            last_run_date, source_cfg_dataset["modified_date_field_id"]
        )

        last_run_timestamp = arrow.get(last_run_date).timestamp * 1000

        if not kn:
            kn = knackpy_wrapper(source_cfg_dataset, auth, filters)

            if kn.data:
                kn.data = filter_by_date(
                    kn.data,
                    source_cfg_dataset["modified_date_field"],
                    last_run_timestamp,
                )
            else:
                #  Replace None with empty list
                kn.data = []

        else:
            kn_temp = knackpy_wrapper(source_cfg_dataset, auth, filters)

            if kn_temp.data:
                kn_temp.data = filter_by_date(
                    kn_temp.data,
                    source_cfg_dataset["modified_date_field"],
                    last_run_timestamp,
                )

            else:
                kn_temp.data = []

            kn.data = kn.data + kn_temp.data
            kn.fields.update(kn_temp.fields)

    return kn


def filter_by_date(data, date_field, compare_date):
    """
    Date field and compare date should be unix timestamps with mills
    
    Args:
        data (TYPE): Description
        date_field (TYPE): Description
        compare_date (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return [record for record in data if record[date_field] >= compare_date]


def cli_args():

    parser = argutil.get_parser(
        "knack_data_pub.py",
        "Publish Knack data to Socrata and ArcGIS Online",
        "dataset",
        "app_name",
        "--destination",
        "--replace",
        "--last_run_date",
    )

    parsed = parser.parse_args()

    return parsed


def main():
    """
    Args:
        None
    
    Returns:
        int: The number of records processed.

    """
    args = cli_args()

    cfg_dataset = cfg[args.dataset]

    last_run_date = args.last_run_date

    auth = KNACK_CREDENTIALS[args.app_name]

    if not last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = "1970-01-01"

    # We include a filter in our API call to limit to records which have
    # been modified on or after the date the last time this job ran
    # successfully. The Knack API supports filter requests by date only
    # (not time), so we must apply an additional filter on the data after
    # we receive it.
    if cfg_dataset.get("multi_source"):
        kn = get_multi_source(cfg_dataset, auth, last_run_date)

    else:

        filters = knackutil.date_filter_on_or_after(
            last_run_date, cfg_dataset["modified_date_field_id"]
        )

        kn = knackpy_wrapper(cfg_dataset, auth, filters=filters)

        if kn.data:
            # Filter data for records that have been modifed after the last
            # job run (see comment above)
            last_run_timestamp = arrow.get(last_run_date).timestamp * 1000
            kn.data = filter_by_date(
                kn.data, cfg_dataset["modified_date_field"], last_run_timestamp
            )

    if not kn.data:
        return 0

    if cfg_dataset.get("fetch_locations"):

        # Optionally fetch location data from another knack view and merge with
        # primary dataset. We access the base cfg_dataset object to pull request info
        # from the 'locations' config.

        locations = knackpy_wrapper(cfg["locations"], auth)

        lat_field = cfg["locations"]["location_fields"]["lat"]
        lon_field = cfg["locations"]["location_fields"]["lon"]

        kn.data = datautil.merge_dicts(
            kn.data,
            locations.data,
            cfg_dataset["location_join_field"],
            [lat_field, lon_field],
        )

    date_fields = [
        kn.fields[f]["label"]
        for f in kn.fields
        if kn.fields[f]["type"] in ["date_time", "date"]
    ]

    if args.destination[0] == "socrata":
        pub = socrata_pub(kn.data, cfg_dataset, args.replace, date_fields=date_fields)

    if args.destination[0] == "agol":
        pub = agol_pub(kn.data, cfg_dataset, args.replace)

    if args.destination[0] == "csv":
        write_csv(kn, cfg_dataset, args.dataset)

    return len(kn.data)


if __name__ == "__main__":
    main()
