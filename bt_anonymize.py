#!/usr/bin/env python


"""
Anonymizes an Individual Address File (IAF) and corresponding
Individual Traffic Match File (ITMF) by replacing actual MAC addresses with
randomly generated MAC addresses.

IAF filename format:

    [AWAM Host Instance Name]_bt_[month-day-year].txt

IAF Columns:

    Host Read Time
    Field Device IP Address
    Field Device Read Time
    Reader Identifier
    Device Address

ITMF filename format:

    [AWAM Host Instance Name]_btmatch_[month-day-year].txt

ITMF Columns:

    Device Address
    Origin Reader Identifier
    Destination Reader Identifier
    Start Time
    End Time
    Travel Time Seconds
    Speed Miles Per Hour
    Match Validity
    Filter Identifier

"""


import random
import csv
import sys
import os
from datetime import datetime
import pytz


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

def random_mac_address():
    _hex_digits = [random.randint(0x00, 0xff) for i in range(5)]
    _address = ':'.join(map(lambda x: "%02x" % x, _hex_digits))
    return(_address)

def randomize(input_dir, month_day_year, output_dir, awam_host_instance_name="Austin"):

    # Set IAF path parameters
    iaf_filename = "%s_bt_%s.txt" % (awam_host_instance_name, month_day_year)
    iaf_input_path = os.path.join(input_dir, iaf_filename)
    iaf_output_path = os.path.join(output_dir, iaf_filename)

    # Set ITMF path parameters
    itmf_filename = "%s_btmatch_%s.txt" % (awam_host_instance_name, month_day_year)
    itmf_input_path = os.path.join(input_dir, itmf_filename)
    itmf_output_path = os.path.join(output_dir, itmf_filename)

    # Create dictionary that maps MAC addresses to their replacements
    newmacs = {}

    # Process the IAF file
    with open(iaf_input_path, 'rb') as iaf_input_file:

        # Read the IAF data
        rows = []
        reader = csv.reader(iaf_input_file)
        for row in reader:

            # Replace the true MAC addresses with the randomly generated one
            mac = row[4]
            if mac not in newmacs:
                newmacs[mac] = random_mac_address()
            row[4] = newmacs[mac]

            # Replace time strings with Unix times
            row[0] = get_timestamp(row[0])
            row[2] = get_timestamp(row[2])

            # Add a unique row identifier
            row.insert(0, row[0] + row[4])

            # Remove the IP address
            del row[2]

            # Add the modified row
            rows.append(row)

        # Write the anonymized IAF data
        with open(iaf_output_path, 'wb') as iaf_output_file:
            writer = csv.writer(iaf_output_file)
            writer.writerows(rows)

    # Process the ITMF file
    with open(itmf_input_path, 'rb') as itmf_input_file:

        # Read the ITMF data
        rows = []
        reader = csv.reader(itmf_input_file)
        for row in reader:

            # Replace the MAC addresses
            mac = row[0]
            if mac not in newmacs:
                newmacs[mac] = random_mac_address()
            row[0] = newmacs[mac]
            rows.append(row)

            # Replace time strings with Unix times
            row[3] = get_timestamp(row[3])
            row[4] = get_timestamp(row[4])

            # Add a unique row identifier
            row.insert(0, row[3] + row[0])

        # Write the anonymized ITMF data
        with open(itmf_output_path, 'wb') as itmf_output_file:
            writer = csv.writer(itmf_output_file)
            writer.writerows(rows)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    month_day_year = sys.argv[2]
    output_dir = sys.argv[3]
    randomize(input_dir, month_day_year, output_dir)