#!/usr/bin python

import os
import sys
from socrata_helpers import upsert_data
import csv
from secrets import SOCRATA_CREDENTIALS

def upsert(input_dir, month_day_year, awam_host_instance_name="Austin"):

    # Set ITMF path parameters
    # itmf_filename = "%s_btmatch_%s.txt" % (awam_host_instance_name, month_day_year)
    itmf_filename = "%s_btmatch_%s.csv" % (awam_host_instance_name, month_day_year)
    itmf_input_path = os.path.join(input_dir, itmf_filename)

    # Upload the data
    with open(itmf_input_path, 'r') as itmf_input_file:
        reader = csv.reader(itmf_input_file)
        fieldnames = [
            'record_id','device_address','origin_reader_identifier','destination_reader_identifier','start_time',
            'end_time','travel_time_seconds','speed_miles_per_hour','match_validity','filter_identifier', 'day_of_week'
        ]

        # Skip header row
        reader.next()
        data = [dict(zip(fieldnames, record)) for record in reader]
        upsert_data(SOCRATA_CREDENTIALS, data, "x44q-icha")


if __name__ == '__main__':
    input_dir = sys.argv[1]
    month_day_year = sys.argv[2]
    upsert(input_dir, month_day_year)
