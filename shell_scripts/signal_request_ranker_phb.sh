#!/bin/bash
cd ../data_tracker
source activate datapub1
python signal_request_ranker.py phb data_tracker_prod
source deactivate