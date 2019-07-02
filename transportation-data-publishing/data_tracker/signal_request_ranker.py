# Assign traffic and PHB request rankings based on evaluation score
# dataset argument is required and must be either 'phb' or 'traffic_signal'

import pdb

import arrow
import knackpy
import argutil
import datautil

import _setpath
from config.secrets import *
from config.knack.config import SIGNAL_REQUEST_RANKER as cfg


def main():

    args = cli_args()

    app_name = args.app_name

    eval_type = args.eval_type

    obj = cfg["eval_types"][eval_type]

    knack_creds = KNACK_CREDENTIALS[app_name]

    kn = knackpy.Knack(
        obj=cfg["eval_types"][eval_type],
        app_id=knack_creds["app_id"],
        api_key=knack_creds["api_key"],
    )

    data = datautil.filter_by_val(kn.data, cfg["status_key"], cfg["status_vals"])

    #  new records will not have a score key. add it here.
    data = datautil.add_missing_keys(data, {cfg["score_key"]: 0})

    #  create a ranking month_year field
    data = datautil.concat_key_values(data, cfg["concat_keys"], cfg["group_key"], "_")

    knack_data_exclude = [
        record for record in data if record["EXCLUDE_FROM_RANKING"] == True
    ]
    knack_data_include = [
        record for record in data if record["EXCLUDE_FROM_RANKING"] == False
    ]

    #  create list of scores grouped by group key
    score_dict = {}

    for row in knack_data_include:
        key = row[cfg["group_key"]]
        score = int(row[cfg["score_key"]])

        if key not in score_dict:
            score_dict[key] = []

        score_dict[key].append(score)

    for key in score_dict:
        score_dict[key].sort()
        score_dict[key].reverse()

    #  get score rank and append record to payload
    payload = []

    for record in knack_data_include:
        score = int(record[cfg["score_key"]])
        key = record[cfg["group_key"]]
        rank = (
            datautil.min_index(score_dict[key], score) + 1
        )  #  add one because list indices start at 0

        if cfg["rank_key"] in record:
            if record[cfg["rank_key"]] != rank:
                record[cfg["rank_key"]] = rank
                record[cfg["modified_date_key"]] = datautil.local_timestamp()
                payload.append(record)

        else:
            record[cfg["rank_key"]] = rank

    #  assign null ranks to records flagged as exclude from ranking
    for record in knack_data_exclude:

        if cfg["rank_key"] in record:
            #  update excluded records if rank found
            if record[cfg["rank_key"]] != "":
                record[cfg["rank_key"]] = ""
                record[cfg["modified_date_key"]] = datautil.local_timestamp()
                payload.append(record)

    if payload:
        payload = datautil.reduce_to_keys(
            payload, [cfg["rank_key"], "id", cfg["modified_date_key"]]
        )

        payload = datautil.replace_keys(payload, kn.field_map)

        update_response = []

        count = 0
        for record in payload:
            count += 1

            print("Updating record {} of {}".format(count, len(payload)))

            res = knackpy.record(
                record,
                obj_key=obj,
                app_id=knack_creds["app_id"],
                api_key=knack_creds["api_key"],
                method="update",
            )

            update_response.append(res)

        return len(payload)

    else:
        return 0


def cli_args():
    parser = argutil.get_parser(
        "signal_requests_ranker.py",
        "Assign traffic and PHB request based on evaluation score.",
        "eval_type",
        "app_name",
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
