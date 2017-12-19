#!/bin/bash
cd ../open_data
source activate datapub1
python knack_data_pub.py work_orders data_tracker_prod -socrata
source deactivate
