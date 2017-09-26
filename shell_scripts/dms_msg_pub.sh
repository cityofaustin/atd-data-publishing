#!/bin/bash
cd ../open_data
source activate datapub1
python dms_msg_pub.py
source deactivate
