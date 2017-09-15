#!/bin/bash
cd ../data_tracker
source activate datapub1
python signal_pm_copier.py data_tracker_prod
source deactivate
