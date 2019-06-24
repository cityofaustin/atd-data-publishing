"""Extract data from postgrest database and publish to Socrata, ArcGIS Online, 
or CSV.

By default, destination data is incrementally updated based on a
modified date field defined in the configuration file. Alternatively use
--replace to truncate/replace the entire dataset. CSV output is always handled
with --replace.
"""
import argparse
from datetime import datetime
import pdb
import sys

from pypgrest import Postgrest
import argutil
import datautil
import socratautil

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
    args = cli_args()

    cfg_dataset = cfg[args.dataset]

    limit = cfg_dataset.get("limit")

    if not args.last_run_date or args.replace:
        last_run_date = "1970-01-01T00:00:00"
    else:
        last_run_date = datetime.utcfromtimestamp(int(args.last_run_date))
        last_run_date = last_run_date.isoformat()

    pgrest = Postgrest(cfg_dataset["pgrest_base_url"], auth=JOB_DB_API_TOKEN)
    '''
    The `interval` is the number of records which will be processed on each loop.
    It servers as the `offset` paramenter, so it's the means by which we chunk
    records.

    1000 matches the max records of returned by our postgres instance,
    so each loop = 1 request to the source db (postgrest). We have disabled
    pagination in our requests (see below), so it's very important that our
    `offset` does not exceed the number of records the API will return
    in one request.
    '''
    interval = 1000

    offset = 0

    records_processed = 0

    while True:
        '''
        Download records in chunks, posting each chunk to socrata.

        Note that the `order` param ensures that each offset request
        returns the expected chunk of records, by preserving the order
        in which records are returned. Ordering slows down the response
        from the postgREST, but it's necessary to ensure consistent
        results.
        '''
        params = {
            cfg_dataset["modified_date_field"]: f"gte.{last_run_date}",
            "limit": limit,
            "order": "{}.asc".format(cfg_dataset["modified_date_field"]),
            "offset" : offset
        }

        '''
        We disable `pagination` in this request because we are simulating
        pagination by manually passing an `offest` to each request.
        '''
        records = pgrest.select(params=params, pagination=False)

        print("got {} records".format(len(records)))
        
        if not records:
            break

        if args.destination[0] == "socrata":
            date_fields = cfg_dataset.get("date_fields")

            pub = socrata_pub(records, cfg_dataset, args.replace, date_fields=date_fields)
            
            print("Published {} records.".format(len(records)))

        offset += interval

        records_processed += len(records)

    return records_processed


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