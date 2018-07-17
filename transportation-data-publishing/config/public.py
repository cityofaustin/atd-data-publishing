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
            "subject_t": "Data Bakup Exception {}",
            "subject_v": "app_name",
            "loggerresult": "{} records downloaded",
            "scriptid_flag": False

        },
    "detection_status_signals":
        {
            "arguments": ["app_name"],
            "argdescription": "Assign detection status to traffic signal based on status of its detectors.",
            "objects": [],
            "source": "knack",
            "destination":"knack",
            "subject_t": "Detection Status Update Failure {}",
            "subject_v": "app_name",
            "loggerresult": "{} signal records updated",
            "scriptid_flag": False

        },
    "device_status":
        {
            "arguments": ["device_type", "app_name", "--json", "--replace"],
            "argdescription": "",
            "objects": [],
            "source": "knack",
            "destination": "knack",
            "subject_t": "Device Status Check Failure: {}",
            "subject_v": "device_type",
            "loggerresult": "",
            "scriptid_flag": True,
            "id_elements": ["script_name", "device_type"]

        },
    "device_status_log":
        {
            "arguments": ["script_name", "device_type", "app_name"],
            "argdescription": "Generate connectivity statistics and upload to Knack application.",
            "objects":[],
            "source": "knack",
            "destination": "knack",
            "scriptid_flag": True,
            "id_elements": ["script_name", "device_type"],
            "subject_t": "Device Status Log Failure: {}",
            "subject_v": "device_type",
            "loggerresult": "",
        },
    "dms_msg_pub":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "kits",
            "destination": "knack",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "DATA PROCESSING ALERT: DMS Message Update",
            "subject_v": "",
            "loggerresult": ""
        },
    "esb_xml_gen":
        {
            "arguments": ["script_name"],
            "argdescription": "Generate XML message to update 311 Service Reqeusts via Enterprise Service Bus.",
            "objects":[],
            "source": "knack",
            "destination": "XML",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "esb_xml_send":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "fulc":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "kits_cctv_push":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "location_updater":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "markings_agol":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "metadata_updater":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "secondary_signals_updater":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "signal_pm_copier":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "signal_request_ranker":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "street_seg_updater":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "task_orders":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "tcp_business_days":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "traffic_reports":
        {
            "arguments": ["script_name"],
            "argdescription": " ",
            "objects":[],
            "source": "",
            "destination": "",
            "scriptid_flag": False,
            "id_elements": [],
            "subject_t": "",
            "subject_v": "",
            "loggerresult": ""
        },
    "knack_data_pub":
        {
            "arguments": ["dataset", "app_name", "--destination", "--replace"],
            "argdescription":"Publish Knack data to Socrata and ArcGIS Online",
            "id_elements": ["script_name", "dataset", "source", "destination"],
            "source": "knack",
            "subject_t": "Knack Data Pub Failure: {}",
            "subject_v": "dataset",
            "logger_result": "args:{}",
            "scriptid_flag": True
        }
}