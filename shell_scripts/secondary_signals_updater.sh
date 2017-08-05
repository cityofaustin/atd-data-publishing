#!/bin/bash
cd ../data_tracker
source activate datapub1
python secondary_signals_updater.py
source deactivate
