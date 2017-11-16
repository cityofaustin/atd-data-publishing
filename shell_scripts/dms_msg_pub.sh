#!/bin/bash
cd ../data_tracker
source activate datapub1
python dms_msg_pub.py
source deactivate
