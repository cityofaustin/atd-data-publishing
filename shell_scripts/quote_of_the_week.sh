#!/bin/bash
cd ../open_data
source activate datapub1
python knack_data_pub.py quote_of_the_week data_tracker_prod -socrata
source deactivate
