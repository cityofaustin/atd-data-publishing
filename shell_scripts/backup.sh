#!/bin/bash
cd ../data+tracker
source activate datapub1
python backup.py
source deactivate
