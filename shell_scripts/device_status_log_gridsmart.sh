#!/bin/bash
cd ../data_tracker
source activate datapub1
python device_status_log.py gridsmart data_tracker_prod
source deactivate
