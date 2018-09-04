"""Extract data from postgrest database and publish to Socrata, ArcGIS Online, 
or CSV.

By default, destination data is incrementally updated based on a
modified date field defined in the configuration file. Alternatively use
--replace to truncate/replace the entire dataset. CSV output is always handled
with --replace.

#TODO: agol pub

"""
import argparse
import pdb
import sys
import urllib.parse

import arrow
from tdutils import argutil
from tdutils import datautil
from tdutils import pgrestutil
from tdutils import socratautil

import _setpath
from config.postgrest.config import PGREST_PUB as cfg
from config.secrets import *

def after_date_query(date_field, date):
    """Summary
    
    Args:
        date_field (TYPE): Description
        date (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return f"{date_field}=gte.{date}"


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
        source="postgrest",
        replace=replace,
    )


def main():
    """
    Args:
        args (list, required): Command line arguments.
    
    Returns:
        int: The number of records processed.
        
    """
    args = cli_args()

    cfg_dataset = cfg[args.dataset]

    last_run_date = args.last_run_date

    if not last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = "1970-01-01"

    last_run_date = urllib.parse.quote_plus(arrow.get(last_run_date).format())

    pgrest = pgrestutil.Postgrest(cfg_dataset["base_url"], auth=JOB_DB_API_TOKEN)

    query_string = after_date_query(cfg_dataset["modified_date_field"], last_run_date)
    records = pgrest.select(query_string)

    if not records:
        return 0

    date_fields = [
        "traffic_report_status_date_time",
        "published_date",
    ]  # TODO: extract from API definition

    if args.destination[0] == "socrata":
        pub = socrata_pub(records, cfg_dataset, args.replace, date_fields=date_fields)

    return len(records)


def cli_args():
    parser = argutil.get_parser(
        "pgrest_data_pub.py",
        "Publish PostgREST data to Socrata and ArcGIS Online",
        "dataset",
        "--destination",
        "--replace",
    )

    parser.add_argument(
        "-l",
        "--last_run_date",
        type=int,
        required=False,
        help="A unix timestamp representing the last date the job was run. Will be applied as a temporal filter when querying data for processing.",
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
