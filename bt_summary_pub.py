import os
import csv
from datetime import datetime
import pytz
import socrata_helpers
import secrets
import pdb

resouce_id = 'v7zg-5jg9'

fieldnames = ['record_id', 'origin_reader_identifier', 'destination_reader_identifier', 'origin_roadway', 'origin_cross_street', 'origin_direction', 'destination_roadway', 'destination_cross_street', 'destination_direction', 'segment_length_miles', 'timestamp', 'average_travel_time_seconds', 'average_speed_mph', 'summary_interval_minutes', 'number_samples', 'standard_deviation']

rootDir = ''

excluded = 0
included = 0

def has_seconds(local_time_string):
    try:
        datetime.strptime(local_time_string, '%m/%d/%Y %I:%M:%S %p')
        return True

    except ValueError:
        return False

def get_timestamp(local_time_string):
    """
    Convert time string to UTC timestamp.
    """
    # Create timezone object
    local_tz = pytz.timezone("US/Central")

    # Parse time string into naive/timezone-unaware datetime object
    if hasSeconds(local_time_string):
        naive_dt = datetime.strptime(local_time_string, '%m/%d/%Y %I:%M:%S %p')

    else:
        naive_dt = datetime.strptime(local_time_string, '%m/%d/%Y %I:%M %p')


    # Convert naive datetime object to timezone aware datetime object
    local_dt = local_tz.localize(naive_dt)

    # Convert from local timezone to utc timezone
    utc_dt = local_dt.astimezone(pytz.utc)

    # Get Unix time / posix time / epoch time
    epoch = int( utc_dt.timestamp() )

    # Return epoch time
    return(str(epoch))



def process_data(filename):

    with open(filename, 'r') as input_file:
        global included
        global excluded
        
        reader = csv.reader(input_file)

        rows = []

        for row in reader:
            if int(row[13]) > 0:
                included += 1
                row[9] = get_timestamp(row[9])

                # create unique row id
                row.insert(0, '{}{}{}'.format(row[9], row[0], row[1]) )

                rows.append(row)
            else:
                excluded += 1

        print('incuded: {}'.format(included) )
        print('excluded: {}'.format(excluded) )
        
        return rows



for dirpath, subdirs, files in os.walk(rootDir):
    for fname in files:
        if 'Austin_bt_summary_' in fname:
            print(fname)
            data = process_data( os.path.join(dirpath, fname) )
            
            payload = [dict(zip(fieldnames, record)) for record in data]

            socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, payload, resouce_id)




