#!/bin/bash
cd ../open_data
source activate datapub1
python knack_data_pub.py signal_retiming data_tracker_prod -socrata
source deactivate
