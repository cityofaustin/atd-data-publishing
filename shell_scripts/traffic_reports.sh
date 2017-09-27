#!/bin/bash
cd ../data_tracker
source activate datapub1
python traffic_reports.py
source deactivate
