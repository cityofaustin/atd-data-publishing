# Scrape task orders from COA Controller webpage and upload to Data Tracker.

import pdb

import knackpy
from bs4 import BeautifulSoup
import requests
import argutil
import datautil

import _setpath
from config.knack.config import cfg
from config.secrets import *


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


def handle_rows(rows, cols=["DEPT", "TASK_ORDER", "NAME", "ACTIVE"]):
    handled = []

    for row in rows:
        #  janky check to exclude rows that don't match expected schema
        if len(row) == 4:
            handled.append(dict(zip(cols, row)))

    return handled


def compare(new_rows, existing_rows, key="TASK_ORDER"):
    existing_ids = [str(row[key]) for row in existing_rows]
    return [row for row in new_rows if str(row[key]) not in existing_ids]


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

    CONFIG = cfg["task_orders"]
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

    new_rows = compare(rows, kn.data)

    new_rows = datautil.replace_keys(new_rows, kn.field_map)

    for record in new_rows:

        res = knackpy.record(
            record,
            obj_key=CONFIG["ref_obj"][0],
            app_id=KNACK_CREDS["app_id"],
            api_key=KNACK_CREDS["api_key"],
            method="create",
        )

    return len(new_rows)


if __name__ == "__main__":
    main()
