#!/usr/bin/env python


"""
Anonymizes an Individual Address File (IAF) and corresponding
Individual Traffic Match File (ITMF) by replacing actual MAC addresses with
randomly generated MAC addresses.

Example usage:

    python bt_anonymize.py -i /srv/awamdata/ -o /srv/anonymized/ --mdy=01-01-2017

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
import argparse
import hashlib

def get_epoch_time(awam_time_string):
    """
    Convert AWAM time string to Unix time.
    """

    # Create timezone object
    local_tz = pytz.timezone("US/Central")

    # Parse time string into naive/timezone-unaware datetime object
    naive_dt = datetime.strptime(awam_time_string, '%m/%d/%Y %I:%M:%S %p')

    # Convert naive datetime object to timezone aware datetime object
    local_dt = local_tz.localize(naive_dt)

    # Convert from local timezone to utc timezone
    utc_dt = local_dt.astimezone(pytz.utc)

    # Get Unix time / posix time / epoch time
    epoch = utc_dt.strftime('%s')

    # Return epoch time
    return(str(epoch))

def get_iso_time(awam_time_string):
    """
    Convert AWAM time string to ISO time string.
    """

    # Create timezone object
    local_tz = pytz.timezone("US/Central")

    # Parse time string into naive/timezone-unaware datetime object
    naive_dt = datetime.strptime(awam_time_string, '%m/%d/%Y %I:%M:%S %p')

    # Convert naive datetime object to timezone aware datetime object
    local_dt = local_tz.localize(naive_dt)

    # Get local time in ISO format
    iso_local_time = local_dt.isoformat()

    # Return ISO time
    return(iso_local_time)

def get_datetime(awam_time_string):
    """Convert AWAM time string to a timezone-aware datetime object."""

    # Create timezone object
    local_tz = pytz.timezone("US/Central")

    # Parse time string into naive/timezone-unaware datetime object
    naive_dt = datetime.strptime(awam_time_string, '%m/%d/%Y %I:%M:%S %p')

    # Convert naive datetime object to timezone aware datetime object
    local_dt = local_tz.localize(naive_dt)

    # Return the local datetime object
    return(local_dt)

def random_mac_address():
    """
    Generates a random MAC address.
    """
    _hex_digits = [random.randint(0x00, 0xff) for i in range(5)]
    _address = ':'.join(map(lambda x: "%02x" % x, _hex_digits))
    return(_address)

def randomize(input_dir, month_day_year, output_dir, awam_host_instance_name="Austin", header=False):
    """
    Removes personally identifying information from AWAM individual address
    files (IAF) and individual traffic match files (ITMF).
    """

    # Set IAF path parameters
    iaf_input_filename = "%s_bt_%s.txt" % (awam_host_instance_name, month_day_year)
    iaf_input_path = os.path.join(input_dir, iaf_input_filename)
    iaf_output_filename = "%s_bt_%s.csv" % (awam_host_instance_name, month_day_year)
    iaf_output_path = os.path.join(output_dir, iaf_output_filename)

    # Set ITMF path parameters
    itmf_input_filename = "%s_btmatch_%s.txt" % (awam_host_instance_name, month_day_year)
    itmf_input_path = os.path.join(input_dir, itmf_input_filename)
    itmf_output_filename = "%s_btmatch_%s.csv" % (awam_host_instance_name, month_day_year)
    itmf_output_path = os.path.join(output_dir, itmf_output_filename)

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

            # Replace AWAM time strings with ISO formatted time strings
            row[0] = get_epoch_time(row[0])
            row[2] = get_epoch_time(row[2])

            # Remove the IP address
            del row[1]

            # Add a unique row identifier
            row.insert(0, hashlib.md5(str(row)).hexdigest())

            # Add the modified row
            rows.append(row)

        # Write the anonymized IAF data
        with open(iaf_output_path, 'wb') as iaf_output_file:

            # Optionally prepend a header row to the output
            if header:
                headers = ["record_id", "host_read_time", "field_device_read_time", "reader_identifier", "device_address"]
                rows.insert(0, headers)
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

            # Add a day-of-week string (NOTE: Python weekdays are indexed to start on Monday)
            weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            weekday = weekdays[get_datetime(row[3]).weekday()]
            row.append(weekday)

            # Replace AWAM time strings with ISO formatted time strings
            row[3] = get_iso_time(row[3])
            row[4] = get_iso_time(row[4])

            # Add a unique row identifier
            row.insert(0, hashlib.md5(str(row)).hexdigest())

        # Write the anonymized ITMF data
        with open(itmf_output_path, 'wb') as itmf_output_file:

            # Optionally prepend a header row to the output
            if header:
                original_headers = [
                    'device_address','origin_reader_identifier','destination_reader_identifier','start_time',
                    'end_time','travel_time_seconds','speed_miles_per_hour','match_validity','filter_identifier'
                ]
                headers = ['record_id'] + original_headers + ['day_of_week']
                rows.insert(0, headers)
            writer = csv.writer(itmf_output_file)
            writer.writerows(rows)

def cli_args():
    """
    Parse command-line arguments using argparse module.
    """
    parser = argparse.ArgumentParser(prog='bt_anonymize.py', description='Anonymize AWAM data files.')
    # parser.add_argument('--iaf', help="Read an Individual Address File and write an anonymized version to stdout")
    # parser.add_argument('--itmf', help="Read an Individual Traffic-Match File and write an anonymized version to stdout")
    parser.add_argument(
        '--mdy', dest='mdy', required=True,
        help="Specify a Month/Day/Year (mm-dd-yyyy) to anonymize an IAF and corresponding ITMF file"
    )
    parser.add_argument(
        '-i', '--input-dir', dest='inputdir', required=True,
        help="Path to input directory containing IAF and ITMF files"
    )
    parser.add_argument(
        '-o', '--output-dir', dest='outputdir', required=True,
        help="Path to output directory for anonymized IAF and ITMF file"
    )
    args = parser.parse_args()
    return(args)

if __name__ == '__main__':
    args = cli_args()
    input_dir = args.inputdir
    month_day_year = args.mdy
    output_dir = args.outputdir
    randomize(input_dir, month_day_year, output_dir, header=True)
