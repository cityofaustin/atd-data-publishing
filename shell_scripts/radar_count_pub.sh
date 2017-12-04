#!/bin/bash
cd ../open_data
source activate datapub1
python radar_count_pub.py
source deactivate
