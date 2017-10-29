#!/bin/bash
cd ../data_tracker
source activate datapub1
python detection_status_signals.py data_tracker_prod
source deactivate
