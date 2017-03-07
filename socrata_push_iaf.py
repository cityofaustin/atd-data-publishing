import os
import sys
from socrata_helpers import UpsertData
import csv
from secrets import SOCRATA_CREDENTIALS

def upsert(input_dir, month_day_year, awam_host_instance_name="Austin"):

    # Set IAF path parameters
    # iaf_filename = "%s_bt_%s.txt" % (awam_host_instance_name, month_day_year)
    iaf_filename = "%s_bt_%s.csv" % (awam_host_instance_name, month_day_year)
    iaf_input_path = os.path.join(input_dir, iaf_filename)

    # Upload the data
    with open(iaf_input_path, 'r') as iaf_input_file:
        reader = csv.reader(iaf_input_file)
        fieldnames = ['record_id','host_read_time','field_device_read_time','reader_identifier','device_address']
        data = [dict(zip(fieldnames, record)) for record in reader]
        UpsertData(SOCRATA_CREDENTIALS, data, "qnpj-zrb9")

if __name__ == '__main__':
    input_dir = sys.argv[1]
    month_day_year = sys.argv[2]
    upsert(input_dir, month_day_year)
