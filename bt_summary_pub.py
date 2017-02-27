# still need to zip up the rows


# origin_reader_identifier, destination_reader_identifier, origin_roadway, origin_cross_street, origin_direction, destination_roadway, destination_cross_street, destination_direction, segment_length_in_miles, timestamp, average_travel_time_seconds, average_speed_mph, summary_interval_minutes, number_samples, standard_deviation
# congress_slaughter,congress_wm_cannon,Congress,Slaughter,Northbound,Congress,William Cannon,Southbound,1.91,12/30/2015 12:00 AM,-1,-1,15,0,-1

import os
import csv
from datetime import datetime
import pytz
import socrata_helpers
import pdb

resouce_id = 'v7zg-5jg9'

fieldnames = ['origin_reader_identifier', 'destination_reader_identifier', 'origin_roadway', 'origin_cross_street', 'origin_direction', 'destination_roadway', 'destination_cross_street', 'destination_direction', 'segment_length_miles', 'timestamp', 'average_travel_time_seconds', 'average_speed_mph', 'summary_interval_minutes', 'number_samples', 'standard_deviation']

rootDir = './data'

def get_timestamp(local_time_string):
    """
    Convert time string to UTC timestamp.
    """
    # Create timezone object
    local_tz = pytz.timezone("US/Central")

    # Parse time string into naive/timezone-unaware datetime object
    naive_dt = datetime.strptime(local_time_string, '%m/%d/%Y %I:%M:%S %p')

    # Convert naive datetime object to timezone aware datetime object
    local_dt = local_tz.localize(naive_dt)

    # Convert from local timezone to utc timezone
    utc_dt = local_dt.astimezone(pytz.utc)

    # Get Unix time / posix time / epoch time
    epoch = utc_dt.strftime('%s')

    # Return epoch time
    return(str(epoch))



def process_data(filename):

    with open(filename, 'r') as input_file:
        reader = csv.reader(input_file)

        rows = []

        for row in data:
            row[9] = get_timestamp(row[9])

            # create unique row id
            row.insert(0, '{}_{}_{}'.format(row[9], row[0], row[1]) )

            rows.append(row)

        return rows



for dirpath, subdirs, files in os.walk(rootDir):
    for fname in files:
        if 'Austin_bt_summary_' in fname:
            data = process_data( os.path.join(in_dir, infile) )
            
            payload = [dict(zip(fieldnames, record)) for record in data]
            print(payload)
            pdb.set_trace()
