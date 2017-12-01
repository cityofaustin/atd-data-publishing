#!/bin/bash
cd ../traffic_study
source activate datapub1
python traffic_study_loader.py speed
source deactivate
