# Extract radar traffic count data from KITS database and publish
# new records to City of Austin Open Data Portal.

# Attributes:
#     socrata_resource (str): Description
import hashlib
import pdb

import arrow
import knackpy

import _setpath
from config.secrets import *
import argutil
import datautil
import emailutil
import jobutil
import logutil
import kitsutil
import socratautil

# define config variables

socrata_resource = "i626-g7ub"


def my_round(x, base=15):
    """Summary
    
    Args:
        x (TYPE): Description
        base (int, optional): Description
    
    Returns:
        TYPE: Description
    """
    # https://stackoverflow.com/questions/2272149/round-to-5-or-other-number-in-python
    return int(base * round(float(x) / base))


def get_timebin(minute, hour):
    """
    Round an arbitrary minue/hour to the nearest 15 minutes. We expect
    radar count timestamsp to come in at 15 minute increments (this is a device configuration),
    however sometimes they are off by +/- a minute or two).
    
    Args:
        minute (int)
        hour (int)
    
    Returns:
        TYPE: String in 15-minute time format "HH:mm"
    """
    minute = my_round(minute)

    if minute == 60:
        minute = 0
        hour = hour + 1 if hour != 23 else 0

    timebin = "{}:{}".format(hour, minute)
    minute = str(minute).zfill(2)
    hour = str(hour).zfill(2)
    return "{}:{}".format(hour, minute)


def get_direction(lane):
    """Summary
    
    Args:
        lane (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    if "SB" in lane:
        return "SB"
    elif "NB" in lane:
        return "NB"
    elif "EB" in lane:
        return "EB"
    elif "WB" in lane:
        return "WB"
    else:
        return None


def cli_args():
    """Summary
    
    Returns:
        TYPE: Description
    """
    parser = argutil.get_parser(
        "radar_count_pub.py",
        "Publish radar count data from KITS DB to City of Austin Open Data Portal.",
        "--replace",
    )

    args = parser.parse_args()

    return args


def main():
    """Summary
    
    Args:
        job (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Description
    """
    args = cli_args()

    replace = args.replace

    #  get most recent traffic count record from socrata
    socrata_data = socratautil.Soda(
        resource=socrata_resource, soql={"$order": "curdatetime desc", "$limit": 1}
    )

    socrata_data = socrata_data.data

    kits_query_recent = """
        SELECT TOP (1) DETID as det_id
        ,CURDATETIME as dettime
        ,DETNAME as lane
        ,VOLUME as vol
        ,SPEED as spd
        FROM [KITS].[SYSDETHISTORYRM]
        ORDER BY CURDATETIME DESC
        """

    kits_data_recent = kitsutil.data_as_dict(KITS_CREDENTIALS, kits_query_recent)

    for record in kits_data_recent:
        new_date = arrow.get(record["dettime"], "US/Central")
        record["dettime"] = new_date.timestamp

    if replace:

        kits_query = """
            SELECT i.DETID as detid
            ,i.CURDATETIME as curdatetime
            ,i.VOLUME as volume
            ,i.SPEED as speed
            ,i.INTNAME as intname
            ,i.OCCUPANCY as occupancy
            ,e.INTID as int_id
            ,e.DETSN as detname
            FROM [KITS].[SYSDETHISTORYRM] i
            LEFT OUTER JOIN [KITS].[DETECTORSRM] e
            ON i.[DETID] = e.[DETID]
            ORDER BY CURDATETIME DESC
        """

    # send new data if the socrata data is behind KITS data
    # the kits data timestamp is a real unix timestamp (no need to adjust for timezone stupidty)
    elif (
        arrow.get(socrata_data[0]["curdatetime"]).timestamp
        < kits_data_recent[0]["dettime"]
    ):
        # create query for counts since most recent socrata data
        #  query start time must be in local US/Central time (KITSDB is naive!)
        strtime = (
            arrow.get(socrata_data[0]["curdatetime"])
            .to("US/Central")
            .format("YYYY-MM-DD HH:mm:ss")
        )
        #  INTID is KITS_ID in data tracker / socrata
        #  it uniquely identifies the radar device/location
        #  detname and the lane and should be queried from the DETECTORSRM
        #  table note that the values in the detname field in SYSDETHISTORYRM
        #  are not current and appear to be updated only the first time the
        #  detector is configured in KITS
        kits_query = """
            SELECT i.DETID as detid
            ,i.CURDATETIME as curdatetime
            ,i.VOLUME as volume
            ,i.SPEED as speed
            ,i.INTNAME as intname
            ,i.OCCUPANCY as occupancy
            ,e.INTID as int_id
            ,e.DETSN as detname
            FROM [KITS].[SYSDETHISTORYRM] i
            LEFT OUTER JOIN [KITS].[DETECTORSRM] e
            ON i.[DETID] = e.[DETID]
            WHERE (i.[CURDATETIME] >= '{}')
            ORDER BY CURDATETIME DESC
            """.format(
            strtime
        )

    else:
        # No new data
        return 0

    kits_data = kitsutil.data_as_dict(KITS_CREDENTIALS, kits_query)

    print("Processing date/time fields")

    for row in kits_data:
        row["month"] = row["curdatetime"].month
        row["day"] = row["curdatetime"].day
        row["year"] = row["curdatetime"].year
        row["day"] = row["curdatetime"].day
        row["hour"] = row["curdatetime"].hour
        row["minute"] = row["curdatetime"].minute
        row["day_of_week"] = row["curdatetime"].weekday()
        #  day of week is 0 to 6 starting on monday
        #  shit to 0 to 6 starting on sunday
        if row["day_of_week"] == 6:
            row["day_of_week"] = 0
        else:
            row["day_of_week"] = row["day_of_week"] + 1

        row["timebin"] = get_timebin(row["minute"], row["hour"])
        row["direction"] = get_direction(row["detname"].upper())

    kits_data = datautil.replace_timezone(kits_data, "curdatetime")
    kits_data = datautil.iso_to_unix(kits_data, ["curdatetime"])
    kits_data = datautil.stringify_key_values(kits_data)

    hash_fields = ["detid", "curdatetime", "detname"]

    for row in kits_data:
        hasher = hashlib.md5()
        in_str = "".join([str(row[q]) for q in hash_fields])
        hasher.update(in_str.encode("utf-8"))
        row["row_id"] = hasher.hexdigest()

    kits_data = datautil.stringify_key_values(kits_data)

    socrata_payload = datautil.lower_case_keys(kits_data)

    status_upsert_response = socratautil.Soda(
        auth=SOCRATA_CREDENTIALS,
        records=socrata_payload,
        resource=socrata_resource,
        location_field=None,
    )

    return len(socrata_payload)


if __name__ == "__main__":
    main()
