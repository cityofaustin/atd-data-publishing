if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import os
import sys
import pdb
import argparse
import logging
import arrow
from config import config
import kits_helpers
import knack_helpers
import data_helpers
import secrets


fieldmap = {
    # kits_field : data_tracker_field
    "CAMNUMBER" : {
        "knack_id" : "CAMERA_ID",
        "type" : int,
        "detect_changes" : True
    },
    "CAMNAME" : {
        "knack_id" : "LOCATION_NAME",
        "type" : str,
        "detect_changes" : True
    },
        "CAMCOMMENT" : {
        "knack_id" : None,
        "type" : str,
        "detect_changes" : True
    },
        "LATITUDE" : {
        "knack_id" : "LATITUDE",
        "type" : float,
        "detect_changes" : False
    },
        "LONGITUDE" : {
        "knack_id" : "LONGITUDE",
        "type" : float,
        "detect_changes" : False
    },
        "VIDEOIP" : {
        "knack_id" : "CAMERA_IP",
        "type" : str,
        "detect_changes" : True
    },
        "CAMID" : {
        "knack_id" : None,
        "type" : str,
        "detect_changes" : False
    },
        "CAMTYPE" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False,
        "default" : 1
    },
        "CAPTURE" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False
        "default" : 1
    },
        "ISWEBENABLED" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False.
        "default" : 1
    },
        "TECHNOLOGY" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False.
        "default" : 1
    },
}


query = "SELECT * FROM KITSDB.KITS.CAMERA"

def convert_data(data, fieldmap):
    new_data = []

    for record in data:
        new_record = { fieldname : fieldmap[fieldname]['type'](record[fieldname]) for fieldname in record.keys() if fieldname in fieldmap and record[fieldname] }
        new_data.append(new_record)
        
    return new_data


def main(date_time):
    print('starting stuff now')

    kits_data = kits_helpers.data_as_dict(kits_creds, query)

    field_data = knack_helpers.get_fields(knack_objects, knack_creds)
    knack_data = knack_helpers.get_data(knack_scene, knack_view, knack_creds)
    knack_data = knack_helpers.parse_data(knack_data, field_data, convert_to_unix=True)
    field_names = data_helpers.unique_keys(knack_data)
    knack_data = data_helpers.filter_by_key_exists(knack_data, primary_key)

    kits_data_conv = convert_data(kits_data, fieldmap)
    fieldmap_knack_kits = {fieldmap[x]['knack_id'] : x for x in fieldmap.keys() if fieldmap[x]['knack_id'] != None}

    knack_data_repl = data_helpers.replace_keys(knack_data, fieldmap_knack_kits, delete_unmatched=True)
    compare_keys = [key for key in fieldmap.keys() if fieldmap[key]['detect_changes'] ]

    pdb.set_trace()
    bob = data_helpers.detect_changes(kits_data_conv, knack_data_repl, 'CAMNUMBER', keys=compare_keys)
    pdb.set_trace()


def cli_args():
    parser = argparse.ArgumentParser(prog='knack_data_pub.py', description='Publish Knack data to Socrata and ArcGIS Online')
    parser.add_argument('dataset', action="store", type=str, help='Name of the dataset that will be published.')
    args = parser.parse_args()
    return(args)

if __name__ == '__main__':
    args = cli_args()
    dataset = args.dataset

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    #  init logging 
    #  with one logfile per dataset per day
    cur_dir = os.path.dirname(__file__)
    logfile = 'log/kits_sync{}_{}.log'.format(dataset, now_s)
    log_path = os.path.join(cur_dir, logfile)
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logging.info('START AT {}'.format(str(now)))
      
    primary_key = config[dataset]['primary_key']    
    knack_view = config[dataset]['view'] 
    knack_scene = config[dataset]['scene']
    knack_objects = config[dataset]['objects']
    	
    knack_creds = secrets.KNACK_CREDENTIALS
    kits_creds = secrets.KITS_CREDENTIALS


    main(now)
