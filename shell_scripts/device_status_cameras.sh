#!/bin/bash
cd ../data_tracker
source activate datapub1
python device_status.py cameras data_tracker_prod -json
source deactivate
