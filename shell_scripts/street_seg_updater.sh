#!/bin/bash
cd ../data_tracker
source activate datapub1
python street_seg_updater.py
source deactivate
