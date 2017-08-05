#!/bin/bash
cd ../bycle
source activate datapub1
python bcycle_kiosk_pub.py
python bcycle_trip_pub.py
source deactivate