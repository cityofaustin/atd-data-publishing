#!/bin/bash
cd ../open_data
source activate datapub1
python knack_data_pub.py atd_visitor_log visitor_sign_in_prod -socrata -csv
source deactivate
