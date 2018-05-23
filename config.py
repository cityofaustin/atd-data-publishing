'''
Configuration file for automated deployment of transportation-data-publishing scripts.
'''
CRONTAB = '''
#  Crontab entries transportation-data-publishing scripts


'''

#  Shell script template
DOCKER_BASE_CMD = '''
#!/bin/bash
sudo docker run \\
    -d \\
    -v $BUILD_PATH:/app/transportation-data-publishing \\
    --rm \\
    --network=host \\
    --privileged \\
    $IMAGE /bin/bash \\
    -c "$CMD"
'''


#  Script configuration
CONFIG = {
  'default_image' : 'atddocker/tdp',
  'scripts' : [
    {
      'args': [],
      'cron': '50 2 * * *',
      'enabled': True,  #  will ignore if false
      'image': 'atddocker/tdp',  #  exclude this entirely for default
      'name': 'backup',  #  must be unique to config
      'path': 'transportation-data-publishing/data_tracker',  #  relative to repo root
      'script': 'backup.py',
      'comment' : ''  #  optional comments are ignored everywhere
    },
    {
      'args': [],
      'cron': '1 4 * * *',
      'enabled': True,
      'name': 'bcycle_kiosks',
      'path': 'transportation-data-publishing/bcycle',
      'script': 'bcycle_kiosk_pub.py'
    },
    {
      'args': [],
      'cron': '3 4 * * 0',
      'enabled': True,
      'name': 'bcycle_trips',
      'path': 'transportation-data-publishing/bcycle',
      'script': 'bcycle_trip_pub.py'
    },
    {
      'args': ['cabinets', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '05 3 * * *',
      'enabled': True,
      'name': 'cabinets',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['cameras', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '00 * * * *',
      'enabled': True,
      'name': 'cameras',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '50 3 * * *',
      'enabled': True,
      'name': 'detection_status_signals',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'detection_status_signals.py'
    },
    {
      'args': ['detectors', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '10 2 * * *',
      'enabled': True,
      'name': 'detectors',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['cameras', 'data_tracker_prod', '--json'],
      'cron': '30 3 * * *',
      'enabled': True,
      'name': 'device_status_cameras',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status.py'
    },
    {
      'args': ['detectors', 'data_tracker_prod'],
      'cron': '40 3 * * *',
      'enabled': True,
      'name': 'device_status_detectors',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status.py'
    },
    {
      'args': ['gridsmart', 'data_tracker_prod'],
      'cron': '45 3 * * *',
      'enabled': True,
      'name': 'device_status_gridsmart',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status.py'
    },
    {
      'args': ['cameras', 'data_tracker_prod'],
      'cron': '50 3 * * *',
      'enabled': True,
      'name': 'device_status_log_cameras',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status_log.py'
    },
    {
      'args': ['gridsmart', 'data_tracker_prod'],
      'cron': '56 3 * * *',
      'enabled': True,
      'name': 'device_status_log_gridsmart',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status_log.py'
    },
    {
      'args': ['signals', 'data_tracker_prod'],
      'cron': '52 3 * * *',
      'enabled': True,
      'name': 'device_status_log_signals',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status_log.py'
    },
    {
      'args': ['travel_sensors', 'data_tracker_prod'],
      'cron': '54 3 * * *',
      'enabled': True,
      'name': 'device_status_log_travel_sensors',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status_log.py'
    },
    {
      'args': ['signals', 'data_tracker_prod'],
      'cron': '25 3 * * *',
      'enabled': True,
      'name': 'device_status_signals',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status.py'
    },
    {
      'args': ['travel_sensors', 'data_tracker_prod'],
      'cron': '35 3 * * *',
      'enabled': True,
      'name': 'device_status_travel_sensors',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'device_status.py'
    },
    {
      'args': ['dms', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '10 3 * * *',
      'enabled': True,
      'name': 'dms',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': [],
      'cron': '21 * * * *',
      'enabled': True,
      'name': 'dms_msg_pub',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'dms_msg_pub.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '1-59/5 * * * *',
      'enabled': True,
      'name': 'esb_xml_gen',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'esb_xml_gen.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '3-59/5 * * * *',
      'enabled': True,
      'name': 'esb_xml_send',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'esb_xml_send.py'
    },
    {
      'args': ['hazard_flashers', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '15 3 * * *',
      'enabled': True,
      'name': 'hazard_flashers',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': [],
      'cron': '25 * * * *',
      'enabled': True,
      'name': 'kits_cctv_push',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'kits_cctv_push.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '20 * * * *',
      'enabled': True,
      'name': 'location_updater',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'location_updater.py'
    },
    {
      'args': ['pole_attachments', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '35 * * * *',
      'enabled': True,
      'name': 'pole_attachments',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': [],
      'cron': '5-59/15 * * * *',
      'enabled': True,
      'name': 'radar_count_pub',
      'path': 'transportation-data-publishing/open_data',
      'script': 'radar_count_pub.py\n'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '25 2 * * *',
      'enabled': True,
      'name': 'secondary_signals_updater',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'secondary_signals_updater.py'
    },
    { 
      'args': ['signal_request_evals', 'data_tracker_prod', '-d socrata'],
      'cron': '10 * * * *',
      'enabled': True,
      'name': 'signal_request_evals',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': [],
      'cron': '*/2 * * * *',
      'enabled': True,
      'name': 'sig_stat_pub',
      'path': 'transportation-data-publishing/open_data',
      'script': 'sig_stat_pub.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '20 2 * * *',
      'enabled': True,
      'name': 'signal_pm_copier',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'signal_pm_copier.py'
    },
    {
      'args': ['phb', 'data_tracker_prod'],
      'cron': '45 2 * * *',
      'enabled': True,
      'name': 'signal_request_ranker_phb',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'signal_request_ranker.py'
    },
    {
      'args': ['traffic_signal', 'data_tracker_prod'],
      'cron': '40 2 * * *',
      'enabled': True,
      'name': 'signal_request_ranker_traffic_signals',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'signal_request_ranker.py'
    },
    {
      'args': ['signal_requests', 'data_tracker_prod', '-d agol'],
      'cron': '40 * * * *',
      'enabled': True,
      'name': 'signal_requests',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['signal_retiming', 'data_tracker_prod', '-socrata'],
      'cron': '30 * * * *',
      'enabled': True,
      'name': 'signal_retiming',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['signals', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '15 * * * *',
      'enabled': True,
      'name': 'signals',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': [],
      'cron': '*/15 * * * *',
      'enabled': True,
      'name': 'street_seg_updater',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'street_seg_updater.py'
    },
    {
      'args': ['timed_corridors', 'data_tracker_prod', '-d socrata --replace'],
      'cron': '35 2 * * *',
      'enabled': True,
      'name': 'timed_corridors',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py',
      'comment' : 'Always replace this datastet because we don\'t know for certain when it was modified'
    },
    {
      'args': [],
      'cron': '*/5 * * * *',
      'enabled': True,
      'name': 'traffic_reports',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'traffic_reports.py'
    },
    {
      'args': ['traffic_reports', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '4-59/5 * * * *',
      'enabled': True,
      'name': 'traffic_reports_pub',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['travel_sensors', 'data_tracker_prod', '-d socrata', '-d agol'],
      'cron': '15 2 * * *',
      'enabled': True,
      'name': 'travel_sensors',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['atd_visitor_log', 'visitor_sign_in_prod', '-d socrata', '-d csv'],
      'cron': '00 2 * * *',
      'enabled': True,
      'name': 'visitor_log',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': ['work_orders', 'data_tracker_prod', '-d socrata'],
      'cron': '50 * * * *',
      'enabled': True,
      'name': 'work_orders',
      'path': 'transportation-data-publishing/open_data',
      'script': 'knack_data_pub.py'
    },
    {
      'args': [],
      'cron': '50 3 * * *',
      'enabled': True,
      'name': 'traffic_study_locations',
      'path': 'transportation-data-publishing/traffic_study',
      'script': 'traffic_study_locations.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '55 3 * * *',
      'enabled': True,
      'name': 'tcp_business_days',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'tcp_business_days.py'
    },
    {
      'args': ['data_tracker_prod'],
      'cron': '04 4 * * *',
      'enabled': True,
      'name': 'metadata_updater',
      'path': 'transportation-data-publishing/data_tracker',
      'script': 'metadata_updater.py'
    },
    {
      'args': [],
      'cron': '07 */6 * * *',
      'enabled': True,  #  will ignore if false
      'image': 'atddocker/tdp',  #  exclude this entirely for default
      'name': 'task_orders',  #  must be unique to config
      'path': 'transportation-data-publishing/data_tracker',  #  relative to repo root
      'script': 'task_orders.py',
      'comment' : ''  #  optional comments are ignored everywhere
    },
  ]
}











