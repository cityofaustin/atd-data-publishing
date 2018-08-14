ARGUMENTS = {
    "dataset": {
        "action": "store",
        "type": str,
        "help": "Name of the dataset that will be published. Must match entry in Knack config file.",
    },
    "device_type": {
        "action": "store",
        "type": str,
        "choices": ["signals", "travel_sensors", "cameras", "gridsmart"],
        "help": "Type of device to ping.",
    },
    "eval_type": {
        "action": "store",
        "choices": ["phb", "traffic_signal"],
        "type": str,
        "help": "The type of evaluation score to rank.",
    },
    "app_name": {
        "action": "store",
        "choices": ["data_tracker_prod", "data_tracker_test", "visitor_sign_in_prod"],
        "type": str,
        "help": "Name of the knack application that will be accessed",
    },
    "--destination": {
        "flag": "-d",
        "action": "append",
        "choices": ["socrata", "agol", "csv"],
        "required": True,
        "type": str,
        "help": "Destination dataset(s) to which data will be published. Can be repeated for multiple destinations.",
    },
    "--json": {
        "action": "store_true",
        "default": False,
        "help": "Write device data to JSON.",
    },
    "--replace": {
        "flag": "-r",
        "action": "store_true",
        "default": False,
        "help": "Replace all destination data with source data.",
    },
}
