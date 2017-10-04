#!/bin/bash
cd ../open_data
source activate datapub1
python knack_data_pub.py signal_requests data_tracker_prod -agol
source deactivate
