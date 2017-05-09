#  conifguration file for knack_data_pub.py

config = {
    'signals' : {
        'primary_key' : 'SIGNAL_ID',
        'objects' : ['object_11', 'object_12'],
        'scene' : '73',
        'view' : '197',
        'service_url' : 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_signals2/FeatureServer/0/',
        'include_ids' : True,
        'socrata_resource_id' : 'p53x-x73x',
        'pub_log_id' : 'n5kp-f8k4'
    },

    'cameras' : {
        'primary_key' : 'ATD_CAMERA_ID',
        'objects' : ['object_11', 'object_53'],
        'scene' : '144',
        'view' : '395',
        'service_url' : '',
        'include_ids' : True,
        'socrata_resource_id' : 'b4k4-adkb',
        'pub_log_id' : 'n5kp-f8k4'
    },

    'signal_requests' : {
        'primary_key' : 'REQUEST_ID', 
        'objects' : ['object_11', 'object_13'],
        'scene' : '75',
        'view' : '200',
        'include_ids' : True,
        'service_url' : 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_signal_requests/FeatureServer/0/',
        'socrata_resource_id' : '',
        'pub_log_id' : 'n5kp-f8k4'
    },

    'quote_of_the_week' : {
        'primary_key' : None,
        'objects' : ['object_67'],
        'scene' : None,
        'view' : None,
        'service_url' : None,
        'socrata_resource_id' : None,
        'include_ids' : False,
        'pub_log_id' : 'n5kp-f8k4',
        'repo_url' : 'https://api.github.com/repositories/55646931/contents',
        'branch' : 'gh-pages',
        'git_path' : 'components/data/quote_of_the_week.csv'
    }
}


