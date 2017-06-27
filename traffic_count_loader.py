'''
load processed traffic study files into single master csv
it might seem dumb that we're loading these into a csv
but it's the only open-source DB available to us at present


todo:
logging
email alerts
deploy on server
'''
import os
import sys
from shutil import copyfile
import argparse
import csv
import pdb
import logging
import traceback
import arrow
import secrets


config = {
    'VOLUME' : {
        'primary_key' :'TRAFFIC_STUDY_COUNT_ID',
        'source_dir' : secrets.TRAFFIC_COUNT_OUTPUT_VOL_DIR
    },
    'SPEED' : {
        'primary_key' :'TRAFFIC_STUDY_SPEED_ID',
        'source_dir' : secrets.TRAFFIC_COUNT_OUTPUT_SPD_DIR
    },
    'CLASSIFICATION' : {
        'primary_key' :'TRAFFIC_STUDY_CLASS_ID',
        'source_dir' : secrets.TRAFFIC_COUNT_OUTPUT_CLASS_DIR
    }
}


def cli_args():
    parser = argparse.ArgumentParser(prog='traffic_study_loader.py', description='Load traffic study data into master tabular datasets')
    parser.add_argument('dataset', action="store", type=str, help='Name of the dataset that will be published. Available choices are speed, volume, and classification.')
    args = parser.parse_args()
    
    return(args)


def getNewRecords(source_dir, match_key):
    records_new = []
    records_ids = []
    for root, dirs, files in os.walk(source_dir):
        #  collect all records from all files that will be processed
        for name in files:
            file = os.path.join(root, name)
            with open(file, 'r') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    if match_key in row:  #  sanity check for row id
                        #  add all records to a records list that will get written to file
                        records_new.append(row)
                        #  create a list of row ids that will be used to determine if any existing records should be replaced
                        records_ids.append(row[match_key])
        break  #  don't walk subfolders

    return (records_new, records_ids)



def retainRecords(input_csv, match_key, compare_vals):
    records_retain = []
    with open(archive_file, 'r') as infile:
        #  walk through all previously existing records
        #  drop any records that exist in the new records
        reader = csv.DictReader(infile) 
        for row in reader:
            if match_key in row:
                if row[match_key] in compare_vals:  # test if a new version of the record exists in the new records
                    continue
                else:
                    records_retain.append(row)
    return records_retain


def writeData(filename, fieldnames, data, primary_key):
    with open(filename, 'w', newline='\n') as output_file:
        counter = 0
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            counter += 1
            row[primary_key] = counter
            writer.writerow(row)
    return True


def moveFiles(source_dir, dest_dir):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    for root, dirs, files in os.walk(source_dir):
        #  collect all records from all files that will be processed
        for name in files:
            old_file = os.path.join(root, name)
            new_file = os.path.join(dest_dir, name)
            os.rename(old_file, new_file)

        break  #  don't walk subfolders
    return True


def main(primary_key, match_key, source_dir, output_dir, outfile, archive_file):
    
    record_data = getNewRecords(source_dir, match_key)
    records_new = record_data[0]
    records_ids = record_data[1]

    if len(records_new) == 0:  #  stop if no new records
        print('no new records')
        sys.exit()

    fieldnames = list(records_new[0].keys())  #  create fieldname list from first record in records_new
    fieldnames.append(primary_key)  #  add primary key field which does not exist in new records
    
    if os.path.isfile(outfile):
        #  copy existing output to archive
        copyfile(outfile, archive_file)
        records_maintain = retainRecords(archive_file, match_key, records_ids)  #  determine which existing records will be retained

    records_new = records_new + records_maintain
    results = writeData(outfile, fieldnames, records_new, primary_key)

    if results:
        move_files = moveFiles( source_dir, os.path.join(source_dir, 'PROCESSED') )

    return move_files


if __name__ == '__main__':

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    args = cli_args()
    dataset = args.dataset.upper()
    source_dir = config[dataset]['source_dir']
    primary_key = config[dataset]['primary_key']
    match_key = 'ROW_ID'
    output_dir = secrets.TRAFFIC_COUNT_MASTER_DIR
    outfile_name = '{}.csv'.format(dataset)
    outfile = os.path.join(output_dir, outfile_name)
    archive_name = '{}_{}.csv'.format(dataset, now_s)
    archive_file = os.path.join(output_dir, 'archive', archive_name)
    results = main(primary_key, match_key, source_dir, output_dir, outfile, archive_file)