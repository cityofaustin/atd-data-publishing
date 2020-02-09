# Scrape task orders from COA Controller webpage and upload to Data Tracker.


import knackpy
from bs4 import BeautifulSoup
import requests
import argutil
import datautil

import _setpath
from config.knack.config import cfg
from config.secrets import *

# hardcoded sequency matches column order on task order webpage
TK_COLS = ["DEPT", "TASK_ORDER", "NAME", "ACTIVE"]


def get_html(url):
    form_data = {"DeptNumber": 2400, "Search": "Search", "TaskOrderName": ""}
    res = requests.post(url, data=form_data)
    res.raise_for_status()
    return res.text


def handle_html(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")

    parsed = []

    for row in rows:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        parsed.append(cols)

    return parsed


def handle_rows(rows, cols=TK_COLS):
    handled = []

    for row in rows:
        #  janky check to exclude rows that don't match expected schema
        if len(row) == 4:
            handled.append(dict(zip(cols, row)))

    return handled


def handle_bools(rows, col="ACTIVE"):
    # convert yes/no strings to booleans
    for row in rows:
        val = row.get(col)
        row[col] = False if val.lower() == "no" else True

    return rows


def compare(new_rows, existing_rows):
    """
    Identify new/modified task orders
    """
    payload = []

    for new_row in new_rows:
        matched = False

        tk = new_row.get("TASK_ORDER")

        if not tk:
            # this should never happen, TK is priamry key in the finance system
            continue

        for old_row in existing_rows:
            # existing TK found; if any value doesn't match between the old/new,
            # add new record to payload
            if tk == old_row.get("TASK_ORDER"):
                matched = True
                for col in TK_COLS:
                    if old_row.get(col) != new_row.get(col):
                        # grab the knack record id of the existing id so that record
                        # can be updated
                        new_row["id"] = old_row.get("id")
                        payload.append(new_row)
                        break
                break

        if not matched:
            payload.append(new_row)

    return payload


def cli_args():

    parser = argutil.get_parser(
        "task_orders.py",
        "Check controller's office for new task orders and upload to Data Tracker.",
        "app_name",
    )

    args = parser.parse_args()

    return args


def main():

    args = cli_args()
    app_name = args.app_name

    CONFIG = cfg["task_orders"][app_name]

    KNACK_CREDS = KNACK_CREDENTIALS[app_name]

    html = get_html(TASK_ORDERS_ENDPOINT)
    data = handle_html(html)
    rows = handle_rows(data)

    kn = knackpy.Knack(
        scene=CONFIG["scene"],
        view=CONFIG["view"],
        ref_obj=CONFIG["ref_obj"],
        app_id=KNACK_CREDS["app_id"],
        api_key=KNACK_CREDS["api_key"],
    )

    rows = handle_bools(rows)

    new_rows = compare(rows, kn.data)

    payload = datautil.replace_keys(new_rows, kn.field_map)

    for record in payload:

        method = "update" if record.get("id") else "create"

        res = knackpy.record(
            record,
            obj_key=CONFIG["ref_obj"][0],
            app_id=KNACK_CREDS["app_id"],
            api_key=KNACK_CREDS["api_key"],
            method=method,
            timeout=20,
        )

    return len(new_rows)


if __name__ == "__main__":
    main()
