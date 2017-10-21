#!/bin/bash
cd ../data_tracker
source activate datapub1
python device_status.py detectors data_tracker_prod
source deactivate
