#!/bin/bash
cd ../data_tracker
source activate datapub1
python kits_cctv_push.py
source deactivate