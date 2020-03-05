# Copy CCTV records from Data Tracker to KITS traffic management system.

# Attributes:
#     app_name (str): Description
#     fieldmap (TYPE): Description
#     filters (TYPE): Description
#     kits_creds (TYPE): Description
#     kits_table_camera (str): Description
#     kits_table_geom (str): Description
#     kits_table_web (str): Description
#     knack_creds (TYPE): Description
#     knack_objects (list): Description
#     knack_scene (str): Description
#     knack_view (str): Description
#     max_cam_id (int): Description
#     primary_key_knack (str): Description

KITS_CONFIG = {
    "kits_table_geom": "KITSDB.KITS.CameraSpatialData",
    "kits_table_camera": "KITSDB.KITS.CAMERA",
    "kits_table_web": "KITSDB.KITS.WEBCONFIG_MAIN",
    "primary_key_knack": "CAMERA_ID",
    "app_name": "data_tracker_prod",
    "knack_view": "view_395",
    "knack_scene": "view_144",
    "knack_objects": ["object_53", "object_11"],
    "knack_creds": "KNACK_CREDENTIALS",
    "kits_creds": "KITS_CREDENTIALS",
    "max_cam_id": 0,
    "filters": {
        "CAMERA_STATUS": ["TURNED_ON"],
        "CAMERA_MFG": ["Axis", "Sarix", "Spectra Enhanced", "Advidia"],
    },
    "fieldmap": {
        # kits_field : data_tracker_field
        "CAMNUMBER": {
            "knack_id": "CAMERA_ID",
            "type": int,
            "detect_changes": True,
            "table": "KITSDB.KITS.CAMERA",
        },
        "CAMNAME": {
            "knack_id": "CAMNAME",
            "type": str,
            "detect_changes": True,
            "table": "KITSDB.KITS.CAMERA",
        },
        "CAMCOMMENT": {
            "knack_id": None,
            "type": str,
            "detect_changes": False,
            "default": None,
            "table": "KITSDB.KITS.CAMERA",
        },
        "TECHNOLOGY": {
            "knack_id": "TECHNOLOGY", # this field is created by a function in the cctv_push_script
            "type": int,
            "detect_changes": True,
            "default": None,
            "table": "KITSDB.KITS.CAMERA",
        },
        "LATITUDE": {
            "knack_id": "LOCATION_latitude",
            "type": float,
            "detect_changes": False,
            "table": "KITSDB.KITS.CAMERA",
        },
        "LONGITUDE": {
            "knack_id": "LOCATION_longitude",
            "type": float,
            "detect_changes": False,
            "table": "KITSDB.KITS.CAMERA",
        },
        "VIDEOIP": {
            "knack_id": "CAMERA_IP",
            "type": str,
            "detect_changes": True,
            "table": "KITSDB.KITS.CAMERA",
        },
        "CAMID": {
            "knack_id": None,
            "type": str,
            "detect_changes": False,
            "default": None,
            "table": "KITSDB.KITS.CAMERA",
        },
        "CAMTYPE": {
            "knack_id": None,
            "type": int,
            "detect_changes": False,
            "default": 0,
            "table": "KITSDB.KITS.CAMERA",
        },
        "CAPTURE": {
            "knack_id": None,
            "type": int,
            "detect_changes": False,
            "default": 1,
            "table": "KITSDB.KITS.CAMERA",
        },
        "SkipDownload": {
            "knack_id": "DISABLE_IMAGE_PUBLISH",
            "type": bool,
            "detect_changes": True,
            "default": None,
            "table": "KITSDB.KITS.CAMERA",
        },
        "WebID": {
            "knack_id": None,
            "type": int,
            "detect_changes": False,
            "default": None,
            "table": "KITSDB.KITS.WEBCONFIG_MAIN",
        },
        "WebURL": {
            "knack_id": None,
            "type": str,
            "detect_changes": False,
            "default": None,
            "table": "KITSDB.KITS.WEBCONFIG_MAIN",
        },
        "CamID": {
            "knack_id": None,
            "type": int,
            "detect_changes": False,
            "default": None,
            "table": "KITSDB.KITS.CameraSpatialData",
        },
        "GeometryItem": {
            "knack_id": None,
            "type": "geometry",
            "detect_changes": False,
            "default": None,
            "table": "KITSDB.KITS.CameraSpatialData",
        },
    },
}
