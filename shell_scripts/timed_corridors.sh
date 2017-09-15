#!/bin/bash
cd ../open_data
source activate datapub1
python knack_data_pub.py timed_corridors data_tracker_prod -socrata
source deactivate