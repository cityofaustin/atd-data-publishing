#!/bin/bash
cd ../data_tracker
source activate datapub1
python location_updater.py
source deactivate
