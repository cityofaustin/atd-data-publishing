#!/bin/bash
cd ../data_tracker
source activate datapub1
python esb_xml_gen.py data_tracker_prod
source deactivate
