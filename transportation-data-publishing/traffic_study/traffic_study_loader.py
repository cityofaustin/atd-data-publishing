"""
Load processed traffic study files into single master csv.
Use record id hashes to update existing records if needed.
Refresh entire published dataset on open data portal.
"""
import argparse
import csv
import os
import pdb
from shutil import copyfile
import sys
import traceback

import arrow

import _setpath
from config.secrets import *
import emailutil
import logutil
import socratautil


config = {
    "VOLUME": {
        "primary_key": "TRAFFIC_STUDY_COUNT_ID",
        "source_dir": TRAFFIC_COUNT_OUTPUT_VOL_DIR,
        "socrata_resource_id": "jasf-x4rx",
        "fieldnames": [
            "DATETIME",
            "YEAR",
            "MONTH",
            "DAY_OF_MONTH",
            "DAY_OF_WEEK",
            "TIME",
            "COUNT_CHANNEL",
            "CHANNEL",
            "COUNT_TOTAL",
            "DATA_FILE",
            "ROW_ID",
            "SITE_CODE",
            "TRAFFIC_STUDY_COUNT_ID",
        ],
    },
    "SPEED": {
        "primary_key": "TRAFFIC_STUDY_SPEED_ID",
        "source_dir": TRAFFIC_COUNT_OUTPUT_SPD_DIR,
        "socrata_resource_id": "et93-wr2y",
        "fieldnames": [
            "COUNT_TOTAL",
            "SPEED_0_14",
            "SPEED_15_19",
            "SPEED_20_24",
            "SPEED_25_29",
            "SPEED_30_34",
            "SPEED_35_39",
            "SPEED_40_44",
            "SPEED_45_49",
            "SPEED_50_54",
            "SPEED_55_59",
            "SPEED_60_64",
            "SPEED_65_69",
            "SPEED_70_200",
            "DATA_FILE",
            "SITE_CODE",
            "SPEED_CHANNEL",
            "DATETIME",
            "YEAR",
            "MONTH",
            "DAY_OF_MONTH",
            "DAY_OF_WEEK",
            "TIME",
            "ROW_ID",
            "TRAFFIC_STUDY_SPEED_ID",
        ],
    },
    "CLASSIFICATION": {
        "primary_key": "TRAFFIC_STUDY_CLASS_ID",
        "source_dir": TRAFFIC_COUNT_OUTPUT_CLASS_DIR,
        "socrata_resource_id": "2hke-by7g",
        "fieldnames": [
            "COUNT_TOTAL",
            "CLASS_1",
            "CLASS_2",
            "CLASS_3",
            "CLASS_4",
            "CLASS_5",
            "CLASS_6",
            "CLASS_7",
            "CLASS_8",
            "CLASS_9",
            "CLASS_10",
            "CLASS_11",
            "CLASS_12",
            "CLASS_13",
            "DATA_FILE",
            "SITE_CODE",
            "CLASS_CHANNEL",
            "DATETIME",
            "YEAR",
            "MONTH",
            "DAY_OF_MONTH",
            "DAY_OF_WEEK",
            "TIME",
            "ROW_ID",
            "TRAFFIC_STUDY_CLASS_ID",
        ],
    },
}


def cli_args():
    parser = argparse.ArgumentParser(
        prog="traffic_study_loader.py",
        description="Load traffic study data into master tabular datasets",
    )

    parser.add_argument(
        "dataset",
        action="store",
        type=str,
        choices=["speed", "volume", "classification"],
        help="Name of the dataset that will be processed.",
    )

    args = parser.parse_args()

    return args


def getNewRecords(source_dir, match_key):
    records_new = []
    records_ids = []
    for root, dirs, files in os.walk(source_dir):
        #  collect all records from all files that will be processed
        for name in files:
            file = os.path.join(root, name)
            print(file)
            with open(file, "r") as infile:
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
    with open(archive_file, "r") as infile:
        #  walk through all previously existing records
        #  drop any records that exist in the new records
        reader = csv.DictReader(infile)
        for row in reader:
            if match_key in row:
                # test if a new version of the record exists in the new records
                if row[match_key] in compare_vals:
                    continue
                else:
                    records_retain.append(row)
    return records_retain


def writeData(filename, fieldnames, data, primary_key):
    with open(filename, "w", newline="\n") as output_file:
        counter = 0
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in data:
            counter += 1
            # ensure row only contains keys matching fieldnames...gunk happens
            row_filtered = {key: row.get(key) for key in fieldnames}
            row_filtered[primary_key] = counter
            writer.writerow(row_filtered)

    logger.info("{} records processed".format(counter))
    return True


def moveFiles(source_dir, dest_dir):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    for root, dirs, files in os.walk(source_dir):
        for name in files:
            old_file = os.path.join(root, name)
            new_file = os.path.join(dest_dir, name)
            os.rename(old_file, new_file)

        break  #  don't walk subfolders!
    return True


def main(
    primary_key, match_key, source_dir, output_dir, outfile, archive_file, fieldnames
):

    record_data = getNewRecords(source_dir, match_key)
    records_new = record_data[0]
    records_ids = record_data[1]

    if len(records_new) == 0:  #  stop if no new records
        logger.info("No new records.")
        sys.exit()

    #  add primary key field which does not exist in new records
    fieldnames.append(primary_key)
    records_retain = []

    if os.path.isfile(outfile):
        #  copy existing output to archive
        copyfile(outfile, archive_file)

        #  determine which existing records will be retained
        records_retain = retainRecords(archive_file, match_key, records_ids)

    records_new = records_new + records_retain
    results = writeData(outfile, fieldnames, records_new, primary_key)

    if results:
        move_files = moveFiles(source_dir, os.path.join(source_dir, "PROCESSED"))

    if records_new:
        upsert_response = socratautil.replace_resource(
            socrata_creds, socrata_resource_id, records_new
        )

    return move_files


if __name__ == "__main__":
    script = os.path.basename(__file__).replace(".py", "")
    logfile = f"{LOG_DIRECTORY}/{script}.log"
    logger = logutil.timed_rotating_log(logfile)

    now = arrow.now()
    logger.info("START AT {}".format(str(now)))

    now_s = now.format("YYYY_MM_DD")

    args = cli_args()
    logger.info("args: {}".format(str(args)))
    dataset = args.dataset.upper()

    source_dir = config[dataset]["source_dir"]
    primary_key = config[dataset]["primary_key"]
    fieldnames = config[dataset]["fieldnames"]
    match_key = "ROW_ID"
    output_dir = TRAFFIC_COUNT_MASTER_DIR
    outfile_name = "{}.csv".format(dataset)
    outfile = os.path.join(output_dir, outfile_name)
    archive_name = "{}_{}.csv".format(dataset, now_s)
    archive_file = os.path.join(output_dir, "archive", archive_name)

    socrata_creds = SOCRATA_CREDENTIALS
    socrata_resource_id = config[dataset]["socrata_resource_id"]

    try:
        results = main(
            primary_key,
            match_key,
            source_dir,
            output_dir,
            outfile,
            archive_file,
            fieldnames,
        )

    except Exception as e:
        error_text = traceback.format_exc()
        logger.error(error_text)
        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            "Traffic Count Loader Process Failure",
            error_text,
            EMAIL["user"],
            EMAIL["password"],
        )
        raise e

    logger.info("END AT: {}".format(arrow.now().format()))
