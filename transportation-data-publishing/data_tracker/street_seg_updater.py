# Update Knack street segments with data from
# COA ArcGIS Online Street Segment Feature Service
import arrow
import knackpy
import agolutil
import argutil
import datautil
import jobutil
import knackutil

import _setpath
from config.secrets import *
from config.knack.config import STREET_SEG_UPDATER as config


def are_equal(knack_dict, agol_dict):
    # Return True if field values from a knack dict match 
    # values in reference ArcGIS Online dict. Only compare keys from the knack dict
    # that are in the reference dict.
    for key in knack_dict:
        if key in agol_dict:
            if agol_dict[key] == None: # convert NoneTypes from AGOL to match empty strings from Data Tracker
                agol_dict[key] = ''
            
            if str(knack_dict[key]) == str(agol_dict[key]):
                continue
            else:
                return False

    return True



def filter_by_date(data, date_field, compare_date):
    return [record for record in data if record[date_field] >= compare_date]


def cli_args():

    parser = argutil.get_parser(
        "street_seg_updater.py",
        "Update street segment attributes from authoritative GIS layer.",
        "app_name",
        "--last_run_date",
    )

    args = parser.parse_args()

    return args


def main():

    args = cli_args()
    app_name = args.app_name
    last_run_date = args.last_run_date

    knack_creds = KNACK_CREDENTIALS[app_name]

    if not last_run_date:
        # replace dataset by setting the last run date to a long, long time ago
        # the arrow package needs a specific date and timeformat
        last_run_date = "1970-01-01"

    filters = knackutil.date_filter_on_or_after(
        last_run_date, config["modified_date_field_id"]
    )
    """
    We include a filter in our API call to limit to records which have
    been modified on or after the date the last time this job ran
    successfully. The Knack API supports filter requests by date only
    (not time), so we must apply an additional filter on the data after
    we receive it.
    """
    kn = knackpy.Knack(
        scene=config["scene"],
        view=config["view"],
        ref_obj=config["ref_obj"],
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
        filters=filters,
    )

    if kn.data:
        # Filter data for records that have been modifed after the last
        # job run (see comment above)
        last_run_timestamp = arrow.get(last_run_date).timestamp * 1000
        kn.data = filter_by_date(
            kn.data, config["modified_date_field"], last_run_timestamp
        )

    payload = []
    unmatched_segments = []

    if not kn.data:
        # logger.info('No records to update.')
        return 0

    for street_segment in kn.data:

        token = agolutil.get_token(AGOL_CREDENTIALS)
        features = agolutil.query_atx_street(
            street_segment[config["primary_key"]], token
        )

        if features.get("features"):
            if len(features["features"]) > 0:
                segment_data = features["features"][0]["attributes"]
            else:
                unmatched_segments.append(street_segment[config["primary_key"]])
                continue
        else:
            unmatched_segments.append(street_segment[config["primary_key"]])
            continue

        #  we don't want to compare modified dates
        #  because we don't keep that value in sync with the source data on AGOL
        #  because we use our own modified date set in the data tracker
        segment_data.pop(config["modified_date_field"])
        street_segment.pop(config["modified_date_field"])
        
        #  compare new data (segment data) against old (street_segment)
        #  we only want to upload values that have changed
        if not are_equal(street_segment, segment_data):
            segment_data["id"] = street_segment["id"]
            segment_data[config["modified_date_field"]] = datautil.local_timestamp()
            payload.append(segment_data)

    payload = datautil.reduce_to_keys(payload, kn.fieldnames)
    payload = datautil.replace_keys(payload, kn.field_map)

    update_response = []
    count = 0

    for record in payload:
        count += 1

        print("updating record {} of {}".format(count, len(payload)))

        #  remove whitespace from janky Esri attributes
        for field in record:
            if type(record[field]) == str:
                record[field] = record[field].strip()

        res = knackpy.record(
            record,
            obj_key=config["ref_obj"][0],
            app_id=knack_creds["app_id"],
            api_key=knack_creds["api_key"],
            method="update",
        )

        update_response.append(res)

    if len(unmatched_segments) > 0:
        error_text = "Unmatched street segments: {}".format(
            ", ".join(str(x) for x in unmatched_segments)
        )

        raise Exception(error_text)

    return count


if __name__ == "__main__":
    main()