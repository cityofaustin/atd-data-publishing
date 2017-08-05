#  conifguration file for knack_data_pub.py

cfg = {
    'atd_visitor_log' : {
        'obj' : None,
        'primary_key' : 'id',
        'scene' : 'scene_20',
        'view' : 'view_39',
        'ref_obj' : ['object_1'],
        'socrata_resource_id' : 'tkk5-uugs',
        'pub_log_id' : 'n5kp-f8k4',
    },
    'signals' : {
        'primary_key' : 'SIGNAL_ID',
        'obj' : None,
        'scene' : 'scene_73',
        'view' : 'view_197',
        'ref_obj' : ['object_12', 'object_11'],
        'service_url' : 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_signals2/FeatureServer/0/',
        'include_ids' : True,
        'socrata_resource_id' : 'p53x-x73x',
        'pub_log_id' : 'n5kp-f8k4',
        'ip_field' : 'CONTROLLER_IP',
        'location_fields' : {
            'lat' : 'LOCATION_latitude',
            'lon' : 'LOCATION_longitude'
        }
    },
    'cameras' : {
        'primary_key' : 'CAMERA_ID',
        'ref_obj' : ['object_53', 'object_11'],
        'obj' : None,
        'scene' : 'scene_144',
        'view' : 'view_395',
        'service_url' : 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_traffic_cameras/FeatureServer/0/',
        'include_ids' : True,
        'socrata_resource_id' : 'b4k4-adkb',
        'pub_log_id' : 'n5kp-f8k4',
        'ip_field' : 'CAMERA_IP',
        'location_fields' : {
            'lat' : 'LOCATION_latitude',
            'lon' : 'LOCATION_longitude'
        }
    },
    'travel_sensors' : {
        'primary_key' : 'ATD_SENSOR_ID', 
        'ref_obj' : ['object_56', 'object_11'],
        'obj' : None,
        'scene' : 'scene_188',
        'view' : 'view_540',
        'include_ids' : True,
        'service_url' : 'https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/travel_sensors/FeatureServer/0/',
        'socrata_resource_id' : '6yd9-yz29',
        'pub_log_id' : 'n5kp-f8k4',
        'ip_field' : 'SENSOR_IP',
        'location_fields' : {
            'lat' : 'LOCATION_latitude',
            'lon' : 'LOCATION_longitude'
        }
    },
    'quote_of_the_week' : {
        'primary_key' : 'id',
        'obj' : 'object_67',
        'ref_obj': None,
        'scene' : None,
        'view' : None,
        'service_url' : None,
        'socrata_resource_id' : 'v6ne-h66n',
        'include_ids' : False,
        'pub_log_id' : 'n5kp-f8k4',
    },
    'signal_retiming' : {
        'primary_key' : 'ATD_RETIMING_ID',
        'ref_obj' : ['object_42', 'object_45'],
        'obj' : None,
        'scene' : 'scene_375',
        'view' : 'view_1063',
        'service_url' : None,
        'socrata_resource_id' : 'g8w2-8uap',
        'include_ids' : False,
        'pub_log_id' : 'n5kp-f8k4'
    },
    'detectors' : {
        'primary_key' : 'DETECTOR_ID',
        'ref_obj' : ['object_98', 'object_12'],
        'obj' : None,
        'scene' : 'scene_468',
        'view' : 'view_1333',
        'service_url' : 'https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/traffic_detectors/FeatureServer/0/',
        'socrata_resource_id' : 'qpuw-8eeb',
        'include_ids' : True,
        'pub_log_id' : 'n5kp-f8k4',
        'ip_field' : 'DETECTOR_IP',
        'fetch_locations' : True,
        'location_join_field' : 'SIGNAL_ID',
        'location_fields' : {
            'lat' : 'LOCATION_latitude',
            'lon' : 'LOCATION_longitude'
        }
    },
    'timed_corridors' : {
        'primary_key' : 'ATD_SYNC_SIGNAL_ID',
        'ref_obj' : ['object_12', 'object_42', 'object_43'],
        'obj' : None,
        'scene' : 'scene_277',
        'view' : 'view_765',
        'service_url' : None,
        'socrata_resource_id' : 'efct-8fs9',
        'include_ids' : False,
        'pub_log_id' : 'n5kp-f8k4',
        'fetch_locations' : True,
        'location_join_field' : 'SIGNAL_ID',
        'location_fields' : {
            'lat' : 'LOCATION_latitude',
            'lon' : 'LOCATION_longitude'
        }    
    },
    'locations' : {  
        'obj' : None,
        'ref_obj' : ['object_11'],
        'scene' : 'scene_425',
        'view' : 'view_1201',
        'location_fields' : {
            'lat' : 'LOCATION_latitude',
            'lon' : 'LOCATION_longitude'
        }
    }
}


