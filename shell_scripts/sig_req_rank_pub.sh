#!/bin/bash
cd ../open_data
source activate datapub1
python sig_req_rank_pub.py
source deactivate
