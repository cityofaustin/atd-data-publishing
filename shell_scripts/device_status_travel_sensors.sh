#!/bin/bash
cd ../data_tracker
source activate datapub1
python device_status.py travel_sensors data_tracker_prod
source deactivate
