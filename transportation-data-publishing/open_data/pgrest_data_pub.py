
# Extract data from postgrest database and publish to Socrata, ArcGIS Online, 
# or CSV.

# By default, destination data is incrementally updated based on a
# modified date field defined in the configuration file. Alternatively use
# --replace to truncate/replace the entire dataset. CSV output is always handled
# with --replace.

# #TODO: agol pub

import argparse
from copy import deepcopy
import os
import pdb
import traceback
import urllib.parse

import arrow

import _setpath
from config.postgrest.config import cfg
from config.secrets import *
from tdutils import argutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil
from tdutils import pgrestutil
from tdutils import socratautil


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


def main(job, **kwargs):
    """Summary
    
    Args:
        job (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Description
    """
    # cfg_dataset, job, args

    script_name = kwargs["script_name"]
    dataset = kwargs["dataset"]
    app_name = kwargs["app_name"]
    replace = kwargs["replace"]

    cfg_dataset = cfg[dataset]

    last_run_date = job.most_recent()

    if not last_run_date or kwargs["replace"] or job.destination == "csv":
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = "1900-01-01"

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

    if job.destination == "socrata":
        pub = socrata_pub(records, cfg_dataset, replace, date_fields=date_fields)

    # logger.info("END AT {}".format(arrow.now()))

    return len(records)


def cli_args():
    """Summary
    
    Returns:
        TYPE: Description
    """
    parser = argutil.get_parser(
        "pgrest_data_pub.py",
        "Publish PostgREST data to Socrata and ArcGIS Online",
        "dataset",
        "app_name",
        "--destination",
        "--replace",
    )

    args = parser.parse_args()

    return args
