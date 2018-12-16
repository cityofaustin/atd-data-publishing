"""Extract data from postgrest database and publish to Socrata, ArcGIS Online, 
or CSV.

By default, destination data is incrementally updated based on a
modified date field defined in the configuration file. Alternatively use
--replace to truncate/replace the entire dataset. CSV output is always handled
with --replace.
"""
import argparse
import pdb
import sys

import arrow
from pypgrest import Postgrest
from tdutils import argutil
from tdutils import datautil
from tdutils import socratautil

import _setpath
from config.postgrest.config import PGREST_PUB as cfg
from config.secrets import *


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

    limit = cfg_dataset.get("limit")

    if not args.last_run_date or args.replace:
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = "1970-01-01T00:00:00"
    else:
        last_run_date = arrow.get(args.last_run_date).format()

    pgrest = Postgrest(cfg_dataset["pgrest_base_url"], auth=JOB_DB_API_TOKEN)

    params = {
        cfg_dataset["modified_date_field"]: f"gte.{last_run_date}",
        "limit": limit,
    }

    records = pgrest.select(params=params)

    print("got {} records".format(len(records)))
    
    if not records:
        return 0

    date_fields = cfg_dataset.get("date_fields")

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
        "--last_run_date",
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
