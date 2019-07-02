"""
Transform traffic count files so that they can be
inserted into ArcSDE database
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
out_dir = TRAFFIC_COUNT_OUTPUT_VOL_DIR
row_id_name = "ROW_ID"

fieldnames = [
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
    row_id_name,
    "SITE_CODE",
]
directions = ["NB", "EB", "WB", "SB", "THRUS", "LTS"]


def getFile(path):
    print(path)
    with open(path, "r") as in_file:
        for i, line in enumerate(in_file):
            if i == 0:
                data_file = line.split(",")[1].replace("'", "").strip()

            if i == 1:
                site_code = line.split(",")[1].replace("'", "").strip()

            if "Date,Time" in line:
                reader = csv.DictReader([line] + in_file.readlines())
                return (
                    [row for row in reader],
                    reader.fieldnames,
                    data_file,
                    site_code,
                )
        else:
            print("can't find header row")
            raise Exception

    return "bob"


def splitRowsByDirection(rows):
    new_rows = []

    for row in rows:
        date = row["Date"]
        time = row["Time"]

        date_data = parseDateTime(date, time)

        if (
            "Total" in row.keys()
        ):  #  only files with bi-directional data will have a total
            total = row["Total"]
        else:
            total = None

        for d in directions:
            #  clean up row keys in event of extra whitespace (it happens)
            row = {key.strip(): row[key] for key in row.keys()}

            if d in row.keys():
                new_row = {}
                new_row["CHANNEL"] = d

                for date_field in date_data.keys():
                    new_row[date_field] = date_data[date_field]

                new_row["COUNT_CHANNEL"] = row[d]

                if total:
                    new_row["COUNT_TOTAL"] = total
                else:
                    new_row["COUNT_TOTAL"] = new_row["COUNT_CHANNEL"]

                new_rows.append(new_row)

    return new_rows


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


def appendKeyVal(rows, key, val):
    for row in rows:
        row[key] = val
    return rows


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
            if "VOL.CSV" in name.upper() and "PROCESSED" not in root.upper():
                vol_file = os.path.join(root, name)
                move_file = os.path.join(root, "processed", name)

                file_data = getFile(vol_file)

                rows = file_data[0]
                data_file = file_data[2]
                site_code = file_data[3]

                rows = splitRowsByDirection(rows)
                rows = appendKeyVal(rows, "DATA_FILE", data_file)
                rows = appendKeyVal(rows, "SITE_CODE", site_code)
                rows = createRowIDs(
                    rows, row_id_name, ["DATETIME", "DATA_FILE", "COUNT_CHANNEL"]
                )

                #  acquire study year from first row in data
                try:
                    year = rows[0]["YEAR"]
                except IndexError:
                    print("oops")

                #  file name in format 'fme_{study year}_{original file name ending in .csv}'
                filename = "fme_{}_{}".format(year, name)
                out_path = os.path.join(out_dir, filename)

                #  write to file
                with open(out_path, "w", newline="\n") as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

                #  move processed file to processed dir
                move_dir = os.path.join(root, "processed")

                if not os.path.exists(move_dir):
                    os.makedirs(move_dir)

                move_file = os.path.join(move_dir, name)
                os.rename(vol_file, move_file)

                count += 1

            else:
                continue

    logger.info("{} files processed".format(count))


try:
    main()
    logger.info("END AT: {}".format(arrow.now().format()))

except Exception as e:
    error_text = traceback.format_exc()
    print(error_text)
    logger.error(error_text)
    emailutil.send_email(
        ALERTS_DISTRIBUTION,
        "Traffic Study Volume Process Failure",
        error_text,
        EMAIL["user"],
        EMAIL["password"],
    )
    raise e
