#!/bin/bash
cd ../open_data
source activate datapub1
python sig_stat_pub.py
source deactivate
