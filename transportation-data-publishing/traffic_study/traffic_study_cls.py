"""
Transform traffic study classfication files so that they can be
inserted into ArcSDE database and published as open data
"""
import csv
import hashlib
import os
import pdb
import traceback

import arrow

import _setpath
from config.secrets import *
import emailutil
import logutil

script = os.path.basename(__file__).replace(".py", "")
logfile = f"{LOG_DIRECTORY}/{script}.log"
logger = logutil.timed_rotating_log(logfile)

now = arrow.now()
logger.info("START AT {}".format(str(now)))

root_dir = TRAFFIC_COUNT_TIMEMARK_DIR
out_dir = TRAFFIC_COUNT_OUTPUT_CLASS_DIR
row_id_name = "ROW_ID"
directions = ["NB", "EB", "WB", "SB"]

fieldmap = {
    "Total": "COUNT_TOTAL",
    "Motor Bikes": "CLASS_1",
    "Cars & Trailers": "CLASS_2",
    "2 Axle Long": "CLASS_3",
    "Buses": "CLASS_4",
    "2 Axle 6 Tire": "CLASS_5",
    "3 Axle Single": "CLASS_6",
    "4 Axle Single": "CLASS_7",
    "<5 Axle Double": "CLASS_8",
    "5 Axle Double": "CLASS_9",
    ">6 Axle Double": "CLASS_10",
    "<6 Axle Multi": "CLASS_11",
    "6 Axle Multi": "CLASS_12",
    ">6 Axle Multi": "CLASS_13",
}


def getFile(path):
    """
    Extract report metadata from top of report
    """
    print(path)
    with open(path, "r") as in_file:

        data = {"data_file": [], "site_code": ""}

        append_lines = False
        current_channel = ""

        for i, line in enumerate(in_file):
            if len(line) < 5:  #  check for blank lines
                continue

            if i == 0:
                data["data_file"] = (
                    line.split(",")[1].replace("'", "").strip("\n").strip()
                )

            if i == 1:
                data["site_code"] = (
                    line.split(",")[1].replace("'", "").strip("\n").strip()
                )

            if "CHANNEL" in line.upper():
                append_lines = False
                current_channel = (
                    line.split(",")[1].replace("'", "").strip("\n").strip()
                )
                data[current_channel] = []

            if "Date,Time" in line:
                data["header"] = line
                append_lines = True

            if append_lines:
                data[current_channel].append(line)

    return data


def appendKeyVal(rows, key, val):
    for row in rows:
        row[key] = val
    return rows


def parseDateTime(d, t):
    dt = "{} {} {}".format(d, t, "US/Central")
    dt = arrow.get(dt, "M/D/YYYY h:mm A ZZZ")
    utc = dt.to("utc").isoformat()
    year = dt.to("utc").format("YYYY")
    month = dt.to("utc").format("M")
    day = dt.to("utc").format("DD")
    weekday = dt.to("utc").weekday()
    time = dt.to("utc").format("HH:mm")
    return {
        "DATETIME": utc,
        "YEAR": year,
        "MONTH": month,
        "DAY_OF_MONTH": day,
        "DAY_OF_WEEK": weekday,
        "TIME": time,
    }


def mapFields(rows, fieldmap):
    mapped = []
    for row in rows:
        new_row = {}
        for field in row.keys():
            if field in fieldmap:
                new_row[fieldmap[field]] = row[field]
            else:
                new_row[field] = row[field]
        mapped.append(new_row)
    return mapped


def createRowIDs(rows, hash_field_name, hash_fields):
    hasher = hashlib.sha1()
    for row in rows:
        in_str = "".join([row[q] for q in hash_fields])
        hasher.update(in_str.encode("utf-8"))
        row[hash_field_name] = hasher.hexdigest()
    return rows


def main():
    count = 0

    for root, dirs, files in os.walk(root_dir):
        for name in files:
            if "CLS.CSV" in name.upper() and "PROCESSED" not in root.upper():

                cls_file = os.path.join(root, name)

                data = getFile(cls_file)
                data_file = data["data_file"]
                site_code = data["site_code"]
                data["combined"] = []

                for d in directions:
                    if d in data:
                        """
                        Extract file data for each direction ('channel') in report,
                        and append array of rows in data['combined']
                        """
                        reader = csv.DictReader(data[d])
                        rows = [row for row in reader]
                        data[d] = rows
                        data[d] = appendKeyVal(data[d], "DATA_FILE", data_file)
                        data[d] = appendKeyVal(data[d], "SITE_CODE", site_code)
                        data[d] = appendKeyVal(data[d], "CLASS_CHANNEL", d)
                        data["combined"] = data[d] + data["combined"]

                for row in data["combined"]:
                    #  check for empty rows
                    if not row["Date"].strip():
                        data["combined"].remove(row)
                        continue

                    date = row["Date"]
                    time = row["Time"]
                    date_data = parseDateTime(date, time)

                    for date_field in date_data.keys():
                        row[date_field] = date_data[date_field]

                    del (row["Date"])
                    del (row["Time"])

                data["combined"] = mapFields(data["combined"], fieldmap)

                data["combined"] = createRowIDs(
                    data["combined"],
                    row_id_name,
                    ["DATETIME", "DATA_FILE", "CLASS_CHANNEL"],
                )

                #  acquire fieldnames from first row in data
                fieldnames = [key for key in data["combined"][0].keys()]

                #  acquire study year from first row in data
                year = data["combined"][0]["YEAR"]

                #  file name in format 'fme_{study year}_{original file name ending in .csv}'
                filename = "fme_{}_{}".format(year, name)

                out_path = os.path.join(out_dir, filename)

                #  write to file
                with open(out_path, "w", newline="\n") as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data["combined"])

                #  move processed file to processed dir
                move_dir = os.path.join(root, "processed")

                if not os.path.exists(move_dir):
                    os.makedirs(move_dir)

                move_file = os.path.join(move_dir, name)
                os.rename(cls_file, move_file)

                count += 1

            else:
                continue

    logger.info("{} files processed".format(count))


try:
    main()
    logger.info("END AT: {}".format(arrow.now().format()))

except Exception as e:
    error_text = traceback.format_exc()
    logger.error(error_text)
    emailutil.send_email(
        ALERTS_DISTRIBUTION,
        "Traffic Study Classification Process Failure",
        error_text,
        EMAIL["user"],
        EMAIL["password"],
    )
    raise e
