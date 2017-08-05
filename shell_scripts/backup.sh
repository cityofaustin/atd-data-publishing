#!/bin/bash
cd ../data_tracker
source activate datapub1
python backup.py
source deactivate
