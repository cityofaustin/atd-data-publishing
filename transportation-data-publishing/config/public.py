# not really open to public

SCRIPTINFO = {
    "backup":
        {
            "arguments": ["app_name"],
            "argdescription": "",
            "objects": ['object_87', 'object_93', 'object_77', 'object_53',
                        'object_96', 'object_83', 'object_95', 'object_21',
                        'object_14', 'object_109', 'object_73', 'object_110',
                        'object_15', 'object_36', 'object_11', 'object_107',
                        'object_115', 'object_116', 'object_117', 'object_67',
                        'object_91', 'object_89', 'object_12', 'object_118',
                        'object_113', 'object_98', 'object_102', 'object_71',
                        'object_84', 'object_13', 'object_26', 'object_27',
                        'object_81', 'object_82', 'object_7', 'object_42',
                        'object_43', 'object_45', 'object_75', 'object_58',
                        'object_56', 'object_54', 'object_86', 'object_78',
                        'object_85', 'object_104', 'object_106', 'object_31',
                        'object_101', 'object_74', 'object_94', 'object_9',
                        'object_10', 'object_19', 'object_20', 'object_24',
                        'object_57', 'object_59', 'object_65', 'object_68',
                        'object_76', 'object_97', 'object_108', 'object_140',
                        'object_142', 'object_143', 'object_141', 'object_149'],
            "source": "knack",
            "destination": "csv",
            "subject": "backup error",
            "loggerresult": "{} records downloaded"

        },
    "detection_status_signals":
        {
            "arguments": ["app_name"],
            "argdescription": "Assign detection status to traffic signal based on status of its detectors.",
            "objects": [],
            "source": "knack",
            "destination":"knack",
            "subject": "Detection Status Update Failure",
            "loggerresult": "{} signal records updated"

        },
    "device_status":
        {
            "arguments": ["script_name", "device_type", "app_name"],
            "argdescription": "",
            "objects": [],
            "source": "knack",
            "destination": "knack",
            "subject": "Device Status Check Failure: {}",
            "loggerresult": ""

        },
    "device_status_log":
        {
            "arguments": ["script_name", "device_type", "app_name"],
            "argdescription": "",
            "objects":[],
            "source": "knack",
            "destination": "knack"
        },
    "dms_msg_pub":
        {
            "arguments": None
        },
    "esb_xml_gen":
        {
            "arguments": ["app_name"]
        },
    "esb_xml_send":
        {
            "arguments": ["app_name"]
        },
    "fulc":
        {
            "arguments": None
        },
    "kits_cctv_push":
        {
            "arguments": ["app_name"]
        },
    "location_updater":
        {
            "arguments": ["app_name"]
        },
    "markings_agol":
        {
            "arguments": ["app_name"]
        },
    "metadata_updater":
        {
            "arguments": ["app_name"]
        },
    "secondary_signals_updater":
        {
            "arguments": ["app_name"]
        },
    "signal_pm_copier":
        {
            "arguments": ["app_name"]
        },
    "signal_request_ranker":
        {
            "arguments": ["eval_type", "app_name"]
        },
    "street_seg_updater":
        {
            "arguments": None
        },
    "task_orders":
        {
            "arguments": None
        },
    "tcp_business_days":
        {
            "arguments": None
        },
    "traffic_reports":
        {
            "arguments": None
        }
}

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