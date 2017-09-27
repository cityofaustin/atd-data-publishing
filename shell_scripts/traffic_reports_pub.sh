cd ../open_data
source activate datapub1
python knack_data_pub.py traffic_reports data_tracker_prod -socrata -agol
source deactivate