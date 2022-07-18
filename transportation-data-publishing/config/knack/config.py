cfg = {
    "atd_visitor_log": {
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_48",
        "obj": None,
        "primary_key": "id",
        "scene": "scene_20",
        "view": "view_55",
        "ref_obj": ["object_1"],
        "socrata_resource_id": "tkk5-uugs",
    },
    "backup": {
        "objects": [  # NOTICE: These objects will not be included in backup
            "object_137",  # admin_field_meta
            "object_138",  # admin_object_meta
            "object_95",  # csr_flex_notes
            "object_67",  # quote_of_the_week
            "object_77",  # signal_id_generator
            "object_148",  # street_names
            "object_7",  # street_segments
            "object_83",  # tmc_issues
            "object_58",  # tmc_issues_DEPRECTATED_HISTORICAL_DATA_ONLY
            "object_10",  # Asset editor
            "object_19",  # Viewer
            "object_20",  # System Administrator
            "object_24",  # Program Editor
            "object_57",  # Supervisor | AMD
            "object_65",  # Technician|AMD
            "object_68",  # Quote of the Week Editor
            "object_76",  # Inventory Editor
            "object_97",  # Account Administrator
            "object_151",  # Supervisor | Signs&Markings
            "object_152",  # Technician | Signs & Markings
            "object_155",  # Contractor | Detection
        ]
    },
    "cabinets": {
        "primary_key": "CABINET_ID",
        "ref_obj": ["object_118", "object_12"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1793",
        "obj": None,
        "scene": "scene_571",
        "view": "view_1567",
        "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/cabinet_assets/FeatureServer/0/",
        "service_id": "c3fd3bb177cc4291880bbe8c630ed5c4",
        "include_ids": True,
        "socrata_resource_id": "x23u-shve",
        "ip_field": None,
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "cameras": {
        "include_ids": True,
        "ip_field": "CAMERA_IP",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "location_field": "location",
            "lon": "LOCATION_longitude",
        },
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_714",
        "obj": None,
        "primary_key": "CAMERA_ID",
        "ref_obj": ["object_53", "object_11"],
        "scene": "scene_144",
        "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_traffic_cameras/FeatureServer/0/",
        "service_id": "52f2b5e51b9a4b5e918b0be5646f27b2",
        "socrata_resource_id": "b4k4-adkb",
        "status_field": "CAMERA_STATUS",
        "to_json_fields": ["CAMERA_ID", "CAMERA_IP"],
        "status_filter_comm_status": ["TURNED_ON"],
        "view": "view_395",
    },
    "cameras_not_cell_modem": {
        "include_ids": True,
        "ip_field": "CAMERA_IP",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_714",
        "obj": None,
        "primary_key": "CAMERA_ID",
        "ref_obj": ["object_53"],
        "scene": "scene_144",
        "status_field": "CAMERA_STATUS",
        "view": "view_2348",
    },
    "cameras_cell_modem": {
        "include_ids": True,
        "ip_field": "CAMERA_IP",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_714",
        "obj": None,
        "primary_key": "CAMERA_ID",
        "ref_obj": ["object_53"],
        "scene": "scene_144",
        "status_field": "CAMERA_STATUS",
        "view": "view_2347",
        "timeout": 10,
    },
    "csr_flex_notes": {
        "include_ids": True,
        "modified_date_field": "CREATED_DATE",
        "modified_date_field_id": "field_2775",
        "obj": None,
        "ref_obj": ["object_95"],
        "scene": "scene_923",
        "view": "view_2356",
        "pgrest_base_url": "http://transportation-data-01-58741847.us-east-1.elb.amazonaws.com/csr_flex_notes",
    },
    "detectors": {
        "primary_key": "DETECTOR_ID",
        "ref_obj": ["object_98", "object_12"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1533",
        "obj": None,
        "scene": "scene_468",
        "view": "view_1333",
        "service_url": "https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/traffic_detectors/FeatureServer/0/",
        "service_id": "47d17ff3ce664849a16b9974979cd12e",
        "socrata_resource_id": "qpuw-8eeb",
        "include_ids": True,
        "ip_field": "DETECTOR_IP",
        "fetch_locations": True,
        "location_join_field": "SIGNAL_ID",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "dms": {
        "primary_key": "DMS_ID",
        "ref_obj": ["object_109", "object_11"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1658",
        "obj": None,
        "scene": "scene_569",
        "view": "view_1564",
        "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/dynamic_message_signs/FeatureServer/0/",
        "service_id": "e7104494593d4a44a2529e4044ef7d5d",
        "include_ids": True,
        "socrata_resource_id": "4r2j-b4rx",
        "ip_field": "DMS_IP",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "gridsmart": {
        #  endpoint for device status check only.
        #  data publishing is handled via detectors config
        "primary_key": "DETECTOR_ID",
        "ref_obj": ["object_98", "object_12"],
        "obj": None,
        "scene": "scene_468",
        "view": "view_1791",
        "include_ids": True,
        "ip_field": "DETECTOR_IP",
        "status_field": "DETECTOR_STATUS",
        "status_filter_comm_status": ["OK", "BROKEN", "UNKNOWN"],
    },
    "hazard_flashers": {
        "primary_key": "ATD_FLASHER_ID",
        "ref_obj": ["object_110", "object_11"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1701",
        "obj": None,
        "scene": "scene_568",
        "view": "view_1563",
        "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/hazard_flashers/FeatureServer/0/",
        "service_id": "6c4392540b684d598c72e52206d774be",
        "include_ids": True,
        "socrata_resource_id": "wczq-5cer",
        "ip_field": None,
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "locations": {
        "obj": None,
        "ref_obj": ["object_11"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_508",
        "scene": "scene_425",
        "view": "view_1201",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "mmc_activities": {
        "primary_key": "ATD_ACTIVITY_ID",
        "ref_obj": ["object_75", "object_83"],
        "obj": None,
        "scene": "scene_1075",
        "view": "view_2681",
        "socrata_resource_id": "p7pt-re4k",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_2563",
        "include_ids": False,
    },
    "pole_attachments": {
        "primary_key": "POLE_ATTACH_ID",
        "ref_obj": ["object_120"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1813",
        "obj": None,
        "scene": "scene_589",
        "view": "view_1597",
        "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/pole_attachments/FeatureServer/0/",
        "service_id": "3a5a777f780447db940534b5808d4ba7",
        "include_ids": True,
        "socrata_resource_id": "btg5-ebcy",
        "ip_field": None,
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "signals": {
        "include_ids": True,
        "ip_field": "CONTROLLER_IP",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_205",
        "obj": None,
        "primary_key": "SIGNAL_ID",
        "ref_obj": ["object_12", "object_11"],
        "scene": "scene_73",
        "service_id": "e6eb94d1e7cc45c2ac452af6ae6aa534",
        "socrata_resource_id": "p53x-x73x",
        "status_field": "SIGNAL_STATUS",
        "status_filter_comm_status": ["TURNED_ON"],
        "view": "view_197",
    },
    "signal_requests": {
        "primary_key": "REQUEST_ID",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_217",
        "obj": None,
        "scene": "scene_75",
        "view": "view_200",
        "ref_obj": ["object_11", "object_13"],
        "service_url": "http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_signal_requests/FeatureServer/0/",
        "service_id": "c8577cef82ef4e6a89933a7a216f1ae1",
        "include_ids": True,
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
        "socrata_resource_id": None,
    },
    "signal_request_evals": {
        "socrata_resource_id": "f6qu-b7zb",
        "fetch_locations": True,
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
        "location_join_field": "ATD_LOCATION_ID",
        "multi_source": True,
        "sources": [
            #  knack_data_pub.py supports merging multiple source
            #  datasets to a destination layer
            {
                # traffic signal evals
                "include_ids": True,
                "modified_date_field": "MODIFIED_DATE",
                "modified_date_field_id": "field_659",
                "obj": None,
                "primary_key": "ATD_EVAL_ID",
                "ref_obj": ["object_13", "object_27"],
                "scene": "scene_175",
                "view": "view_908",
            },
            {
                #  phb evals
                "include_ids": True,
                "modified_date_field": "MODIFIED_DATE",
                "modified_date_field_id": "field_715",
                "obj": None,
                "primary_key": "ATD_EVAL_ID",
                "ref_obj": ["object_13", "object_26"],
                "scene": "scene_175",
                "view": "view_911",
            },
        ],
    },
    "task_orders": {
        "data_tracker_prod": {
            "primary_key": "TASK_ORDER",
            "ref_obj": ["object_86"],
            "obj": None,
            "scene": "scene_861",
            "view": "view_2229",
            "include_ids": True,
        },
        "finance_admin_prod": {
            "primary_key": "TASK_ORDER",
            "ref_obj": ["object_32"],
            "obj": None,
            "scene": "scene_84",
            "view": "view_720",
            "include_ids": True,
        },
    },
    "traffic_reports": {
        "primary_key": "TRAFFIC_REPORT_ID",
        "modified_date_field": "TRAFFIC_REPORT_STATUS_DATE_TIME",
        "modified_date_field_id": "field_1966",
        "ref_obj": ["object_121"],
        "obj": None,
        "scene": "scene_614",
        "view": "view_1626",
        "service_id": "444c8a2b4388485283c2968bd99ddf6c",
        "include_ids": True,
        "socrata_resource_id": "dx9v-zd7x",
        "ip_field": None,
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "travel_sensors": {
        "primary_key": "ATD_SENSOR_ID",
        "ref_obj": ["object_56", "object_11"],
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_710",
        "obj": None,
        "scene": "scene_188",
        "view": "view_540",
        "include_ids": True,
        "service_url": "https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/travel_sensors/FeatureServer/0/",
        "service_id": "9776d3e894a74521a7f63443f7becc7c",
        "socrata_resource_id": "6yd9-yz29",
        "ip_field": "SENSOR_IP",
        "status_field": "SENSOR_STATUS",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
        "status_filter_comm_status": ["TURNED_ON"],
    },
    "signal_retiming": {
        "primary_key": "ATD_RETIMING_ID",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1257",
        "ref_obj": ["object_42", "object_45"],
        "obj": None,
        "scene": "scene_375",
        "view": "view_1063",
        "service_url": None,
        "socrata_resource_id": "g8w2-8uap",
        "include_ids": False,
    },
    "timed_corridors": {
        "primary_key": "ATD_SYNC_SIGNAL_ID",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_2557",
        "ref_obj": ["object_12", "object_42", "object_43"],
        "obj": None,
        "scene": "scene_277",
        "view": "view_765",
        "service_url": None,
        "socrata_resource_id": "efct-8fs9",
        "include_ids": False,
        "fetch_locations": True,
        "location_join_field": "SIGNAL_ID",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "work_orders_signals": {
        "primary_key": "ATD_WORK_ORDER_ID",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_1074",
        "obj": None,
        "scene": "scene_683",
        "view": "view_1829",
        "ref_obj": ["object_31", "object_11"],
        "socrata_resource_id": "hst3-hxcz",
        "status_field": "WORK_ORDER_STATUS",
        "location_fields": {
            "lat": "LOCATION_latitude",
            "lon": "LOCATION_longitude",
            "location_field": "location",
        },
    },
    "work_orders_signs_markings": {
        "primary_key": "ATD_WORK_ORDER_ID",
        "modified_date_field": "MODIFIED_DATE",
        "modified_date_field_id": "field_2150",
        "obj": None,
        "scene": "scene_1249",
        "view": "view_2226",
        "ref_obj": ["object_140", "object_11", "object_186"],
        "socrata_resource_id": "",
        "pub_log_id": "",
        "status_field": "WORK_ORDER_STATUS",
    },
}

DETETECTION_STATUS_SIGNALS = {
    "CONFIG_DETECTORS": {
        "scene": "scene_468",
        "view": "view_1333",
        "objects": ["object_98"],
    },
    "CONFIG_SIGNALS": {
        "scene": "scene_73",
        "view": "view_197",
        "objects": ["object_12"],
    },
    "CONFIG_STATUS_LOG": {"objects": ["object_102"]},
    "FIELDMAP_STATUS_LOG": {
        "EVENT": "field_1576",
        "SIGNAL": "field_1577",
        "EVENT_DATE": "field_1578",
    },
    "DET_STATUS_LABEL": "DETECTOR_STATUS",
    "DET_DATE_LABEL": "MODIFIED_DATE",
    "SIG_STATUS_LABEL": "DETECTION_STATUS",
    "SIG_DATE_LABEL": "DETECTION_STATUS_DATE",
}


LOCATION_UPDATER = {
    "filters": {
        "match": "and",
        "rules": [{"field": "field_1357", "operator": "is", "value": "No"}],
    },
    "obj": "object_11",
    "field_maps": {
        #  service name
        "EXTERNAL_cmta_stops": {
            "fields": {
                #  AGOL Field : Knack Field
                "ID": "BUS_STOPS"
            }
        }
    },
    "layers": [
        # layer config for interacting with ArcGIS Online
        # see: http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#//02r3000000p1000000
        {
            "service_name": "BOUNDARIES_single_member_districts",
            "outFields": "COUNCIL_DISTRICT",
            "updateFields": ["COUNCIL_DISTRICT"],  #
            "layer_id": 0,
            "distance": 33,  #  !!! this unit is interpreted as meters due to Esri bug !!!
            "units": "esriSRUnit_Foot",  #  !!! this unit is interpreted as meters due to Esri bug !!!
            #  how to handle query that returns multiple intersection features
            "handle_features": "merge_all",
        },
        {
            "service_name": "BOUNDARIES_jurisdictions",
            #  will attempt secondary service if no results at primary
            "service_name_secondary": "BOUNDARIES_jurisdictions_planning",
            "outFields": "JURISDICTION_LABEL",
            "updateFields": ["JURISDICTION_LABEL"],
            "layer_id": 0,
            "handle_features": "use_first",
        },
        {
            "service_name": "TRANSPORTATION_signal_engineer_areas",
            "outFields": "SIGNAL_ENG_AREA",
            "updateFields": ["SIGNAL_ENG_AREA"],
            "layer_id": 0,
            "handle_features": "use_first",
        },
        {
            "service_name": "EXTERNAL_cmta_stops",
            "outFields": "ID",
            "updateFields": ["BUS_STOPS"],
            "layer_id": 0,
            "distance": 107,  #  !!! this unit is interpreted as meters due to Esri bug !!!
            "units": "esriSRUnit_Foot",  #  !!! this unit is interpreted as meters due to Esri bug !!!
            "handle_features": "merge_all",
            "apply_format": True,
        },
    ],
}


MARKINGS_AGOL = [
    # Knack and AGOL source object defintions.
    # Order of config elements matters! Work orders must be processed before
    # jobs and attachments because work orders are the parent record to both.
    {
        "name": "markings_work_orders",
        "scene": "scene_1249",
        "view": "view_3099",
        "ref_obj": ["object_140", "object_186"],
        "modified_date_field_id": "field_2150",
        "modified_date_field": "MODIFIED_DATE",
        "geometry_service_id": "a78db5b7a72640bcbb181dcb88817652",  #  street segments
        "geometry_layer_id": 0,
        "geometry_record_id_field": "SEGMENT_ID",
        "geometry_layer_spatial_ref": 102739,
        "primary_key": "ATD_WORK_ORDER_ID",
        "service_id": "a9f5be763a67442a98f684935d15729b",
        "layer_id": 1,
        "item_type": "layer",
    },
    {
        "name": "markings_jobs",
        "scene": "scene_1249",
        "view": "view_3100",
        "ref_obj": ["object_141", "object_186"],
        "modified_date_field_id": "field_2196",
        "modified_date_field": "MODIFIED_DATE",
        "geometry_service_id": "internal",  # geometry is pulled from work order data in-memory
        "geometry_record_id_field": "ATD_WORK_ORDER_ID",
        "geometry_layer_spatial_ref": 102739,
        "primary_key": "ATD_SAM_JOB_ID",
        "service_id": "a9f5be763a67442a98f684935d15729b",
        "layer_id": 0,
        "item_type": "layer",
    },
    {
        "name": "attachments",
        "scene": "scene_1249",
        "view": "view_3096",
        "ref_obj": ["object_153"],
        "modified_date_field_id": "field_2407",
        "modified_date_field": "CREATED_DATE",
        "multi_source_geometry": False,
        "primary_key": "ATTACHMENT_ID",
        "service_id": "a9f5be763a67442a98f684935d15729b",
        "layer_id": 0,
        "item_type": "table",
        "extract_attachment_url": True,
    },
    {
        "name": "specifications",
        "scene": "scene_1249",
        "view": "view_3103",
        "ref_obj": ["object_143", "object_140", "object_141"],
        "modified_date_field_id": "field_2567",
        "modified_date_field": "MODIFIED_DATE",
        "primary_key": "SPECIFICATION_ID",
        "service_id": "a9f5be763a67442a98f684935d15729b",
        "layer_id": 1,
        "item_type": "table",
    },
    {
        "name": "materials",
        "scene": "scene_1249",
        "view": "view_3104",
        "ref_obj": ["object_36", "object_140", "object_141", "object_15"],
        "modified_date_field_id": "field_771",
        "modified_date_field": "MODIFIED_DATE",
        "primary_key": "TRANSACTION_ID",
        "service_id": "a9f5be763a67442a98f684935d15729b",
        "layer_id": 2,
        "item_type": "table",
    },
]

SIGNS_AGOL = {
    # Knack and AGOL source object defintions.
    # Order of config elements matters! Work orders must be processed before
    # speccs, materials, etc because work orders are the parent record.
    "work_order_signs_locations": {
        # there is no AGOL feature service for sign locations, instead
        # we merge location attributes to each work_orders_signs_asset_spec_actuals
        "name": "work_order_signs_locations",
        "scene": "scene_1249",
        "view": "view_3105",
        "ref_obj": ["object_177", "object_176"],
        "modified_date_field_id": "field_3383",
        "modified_date_field": "MODIFIED_DATE",
        "primary_key": "LOCATION_ID",
        "geometry_field_name": "SIGNS_LOCATION",
        "service_id": None,
        "layer_id": None,
        "item_type": None,
    },
    "work_orders_signs": {
        "name": "work_orders_signs",
        "scene": "scene_1249",
        "view": "view_3107",
        "ref_obj": ["object_176"],
        "modified_date_field_id": "field_3206",
        "modified_date_field": "MODIFIED_DATE",
        "primary_key": "ATD_WORK_ORDER_ID",
        "service_id": "93e29b23c39b4110ab0bbefde79b4063",
        "layer_id": 1,
        "item_type": "layer",
    },
    "work_orders_signs_asset_spec_actuals": {
        "name": "work_orders_signs_asset_spec_actuals",
        "scene": "scene_1249",
        "view": "view_3106",
        "ref_obj": ["object_181", "object_178", "object_177"],
        "modified_date_field_id": "field_3365",
        "modified_date_field": "MODIFIED_DATE",
        "location_join_field": "LOCATION_ID",
        "work_order_id_field": "ATD_WORK_ORDER_ID",
        "primary_key": "SPECIFICATION_ID",
        "service_id": "93e29b23c39b4110ab0bbefde79b4063",
        "layer_id": 0,
        "item_type": "layer",
    },
    "work_orders_attachments": {
        "name": "work_orders_attachments",
        "scene": "scene_1249",
        "view": "view_3127",
        "ref_obj": ["object_153"],
        "modified_date_field_id": "object_153",
        "modified_date_field": "MODIFIED_DATE",
        "work_order_id_field": "ATD_WORK_ORDER_ID",
        "primary_key": "ATTACHMENT_ID",
        "service_id": "93e29b23c39b4110ab0bbefde79b4063",
        "layer_id": 0,
        "item_type": "table",
        "extract_attachment_url": True,
    },
    "work_orders_materials": {
        "name": "work_orders_materials",
        "scene": "scene_1249",
        "view": "view_3126",
        "ref_obj": ["object_36", "object_176"],
        "modified_date_field_id": "field_771",
        "modified_date_field": "MODIFIED_DATE",
        "work_order_id_field": "ATD_WORK_ORDER_ID",
        "primary_key": "TRANSACTION_ID",
        "service_id": "93e29b23c39b4110ab0bbefde79b4063",
        "layer_id": 1,  # note that this service id is actually 3, but this identifies the table #. not sure where this is exposed or documented. trial and error...
        "item_type": "table",
    },
}


SECONDARY_SIGNALS_UPDATER = {
    "update_field": "field_1329",
    "ref_obj": ["object_12"],
    "scene": "scene_73",
    "view": "view_197",
}


SIGNAL_PM_COPIER = {
    "params_pm": {
        "field_obj": ["object_84", "object_12"],
        "scene": "scene_416",
        "view": "view_1182",
    },
    "params_signal": {
        "field_obj": ["object_12"],
        "scene": "scene_73",
        "view": "view_197",
    },
    "copy_fields": ["PM_COMPLETED_DATE", "WORK_ORDER", "PM_COMPLETED_BY"],
}


SIGNAL_REQUEST_RANKER = {
    "primary_key": "ATD_EVAL_ID",
    "status_key": "EVAL_STATUS",
    "group_key": "YR_MO_RND",
    "score_key": "EVAL_SCORE",
    "concat_keys": ["RANK_ROUND_MO", "RANK_ROUND_YR"],
    "rank_key": "EVAL_RANK",
    "status_vals": ["NEW", "IN PROGRESS", "COMPLETED"],
    "modified_date_key": "MODIFIED_DATE",
    "eval_types": {"traffic_signal": "object_27", "phb": "object_26"},
}


STREET_SEG_UPDATER = {
    "modified_date_field_id": "field_144",
    "modified_date_field": "MODIFIED_DATE",
    "primary_key": "SEGMENT_ID_NUMBER",
    "ref_obj": ["object_7"],
    "scene": "scene_424",
    "view": "view_1198",
}


TCP_BUSINESS_DAYS = {
    "scene": "scene_754",
    "view": "view_1987",
    "obj": "object_147",
    "start_key": "SUBMITTED_DATE",
    "end_key": "REVIEW_COMPLETED_DATE",
    "elapsed_key": "DAYS_ELAPSED",
    "update_fields": ["DAYS_ELAPSED", "id"],
}


PURCHASE_REQUEST_COPIER = {
    "purchase_requests": {
        "scene": "scene_84",
        "view": "view_211",
        "ref_obj": ["object_1"],
        "unique_id_field_name": "AUTO_INCREMENT",
        "copy_field_id": "field_268",
        "requester_field_id": "field_12",
        "copied_by_field_id": "field_283",
    },
    "items": {
        "obj": "object_4",
        "pr_field_id": "field_269",
        "pr_connection_field_name": "purchase_request",
    },
}
SIGNAL_PMS_POSTGRE_KNACK = {
    "form_id": "44359e32-1a7f-41bd-b53e-3ebc039bd21a",
    "postgre_url": "http://transportation-data-01-58741847.us-east-1.elb.amazonaws.com/signal_pms",
    "knack_pms": {"scene": "scene_920", "view": "view_2350", "ref_obj": ["object_84"]},
    "knack_signals": {
        "scene": "scene_73",
        "view": "view_197",
        "ref_obj": ["object_12"],
    },
    "knack_technicians": {"objects": "object_9"},
}

SR_DUE_DATE = {
    "data_tracker": {
        "issues": {
            "scene": "scene_514",
            "view": "view_2351",
            "ref_obj": ["object_83"],
            "due_date_field_id": "field_2772",
            "sr_field_id": "field_1232",
        },
        "flex_notes": {
            "scene": "scene_514",
            "view": "view_2352",
            "ref_obj": ["object_95"],
            "sr_id_field": "field_1452",
            "flex_question_code_field_id": "field_1455",
        },
    },
    "signs_markings": {
        "issues": {
            "scene": "scene_1232",
            "view": "view_3059",
            "ref_obj": ["object_171"],
            "due_date_field_id": "field_3527",
            "sr_field_id": "field_3071",
        },
        "flex_notes": {
            "scene": "scene_1232",
            "view": "view_3060",
            "ref_obj": ["object_172"],
            "sr_id_field": "field_3128",
            "flex_question_code_field_id": "field_3131",
        },
    },
}

SR_ASSET_ASSIGNER = {
    "tmc_issues": {
        "scene": "scene_514",
        "view": "view_2362",
        "ref_obj": ["object_83"],
        "x_field": "CSR_Y_VALUE",
        "y_field": "CSR_X_VALUE",
        "primary_key": "field_1678",  # TMC_ISSUE_ID
        "connection_field_keys": {"signals": "field_1367"},
        "assign_status_field_id": "field_2813",
    },
    "tmc_asset_form": {
        "scene": "scene_428",
        "view": "view_1521",
        "input_fields": ["ASSET_TYPE"],
    },
    "signals": {
        "scene": "scene_73",
        "view": "view_197",
        "ref_obj": ["object_12"],
        "primary_key": "field_199",  # SIGNAL_ID
        "display_name": "Signal",
        "layer": {
            "service_name": "TRANSPORTATION_signals2",
            "outFields": "SIGNAL_ID",
            "layer_id": 0,
            "distance": 10,
            "units": "esriSRUnit_Foot",
            "primary_key": "SIGNAL_ID",
        },
    },
}
