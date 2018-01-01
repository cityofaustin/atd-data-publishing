from copy import deepcopy
import logging
import os
import pdb
import sys
import time

import arrow
import knackpy

import _setpath
from config.secrets import *
from util import kitsutil
from util import datautil
from util import emailutil

kits_table_geom = "KITSDB.KITS.CameraSpatialData"
kits_table_camera = "KITSDB.KITS.CAMERA"
kits_table_web = "KITSDB.KITS.WEBCONFIG_MAIN"


fieldmap = {
    # kits_field : data_tracker_field
    "CAMNUMBER" : {
        "knack_id" : "CAMERA_ID",
        "type" : int,
        "detect_changes" : True,
        "table" : kits_table_camera
    },
    "CAMNAME" : {
        "knack_id" : "CAMNAME",
        "type" : str,
        "detect_changes" : True,
        "table" : kits_table_camera
    },
    "CAMCOMMENT" : {
        "knack_id" : None,
        "type" : str,
        "detect_changes" : False,
        "default" : None,
        "table" : kits_table_camera
    },
    "LATITUDE" : {
        "knack_id" : "LOCATION_latitude",
        "type" : float,
        "detect_changes" : False,
        "table" : kits_table_camera
    },
    "LONGITUDE" : {
        "knack_id" : "LOCATION_longitude",
        "type" : float,
        "detect_changes" : False,
        "table" : kits_table_camera
    },
    "VIDEOIP" : {
        "knack_id" : "CAMERA_IP",
        "type" : str,
        "detect_changes" : True,
        "table" : kits_table_camera
    },
    "CAMID" : {
        "knack_id" : None,
        "type" : str,
        "detect_changes" : False,
        "default" : None,
        "table" : kits_table_camera
    },
    "CAMTYPE" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False,
        "default" : 0,
        "table" : kits_table_camera
    },
    "CAPTURE" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False,
        "default" : 1,
        "table" : kits_table_camera
    },
    "SkipDownload" : {
        "knack_id" : "DISABLE_IMAGE_PUBLISH",
        "type" : bool,
        "detect_changes" : True,
        "default" : None,
        "table" : kits_table_camera
    },
    "WebID" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False,
        "default" : None,
        "table" : kits_table_web
    },
    "WebURL" : {
        "knack_id" : None,
        "type" : str,
        "detect_changes" : False,
        "default" : None,
        "table" : kits_table_web
    },
    "CamID" : {
        "knack_id" : None,
        "type" : int,
        "detect_changes" : False,
        "default" : None,
        "table" : kits_table_geom
    },
    "GeometryItem" : {
        "knack_id" : None,
        "type" : 'geometry',
        "detect_changes" : False,
        "default" : None,
        "table" : kits_table_geom
    }
}


primary_key_knack = 'CAMERA_ID'   
app_name = 'data_tracker_prod'
knack_view = 'view_395'
knack_scene = 'view_144'
knack_objects = ['object_53', 'object_11']
knack_creds = KNACK_CREDENTIALS
kits_creds = KITS_CREDENTIALS

max_cam_id = 0

filters = {
   'CAMERA_STATUS' : ['TURNED_ON'],
   'CAMERA_MFG' : ['Axis', 'Sarix', 'Spectra Enhanced']
}

def map_bools(dicts):
    #  convert boolean values to 1/0 for SQL compatibility 
    for record in dicts:
        for key in record.keys():
            try:
                if fieldmap[key]['type'] == bool:
                    record[key] = int(record[key])
            except KeyError:
                continue

    return dicts

def createCameraQuery(table_name):
    return "SELECT * FROM {}".format(table_name)


def convert_data(data, fieldmap):
    new_data = []

    for record in data:
        new_record = { 
            fieldname : fieldmap[fieldname]['type'](record[fieldname]) 
            for fieldname in record.keys()
            if fieldname in fieldmap and fieldname in record.keys()
        }

        new_data.append(new_record)
        
    return new_data


def setDefaults(dicts, fieldmap):
    for row in dicts:
        for field in fieldmap.keys():

            if (field not in row and
                fieldmap[field]['default'] != None
                and fieldmap[field]['table'] == kits_table_camera):
                
                row[field] = fieldmap[field]['default']

    return dicts


def createCAMCOMMENT(dicts):
    for row in dicts:
        row['CAMCOMMENT'] = 'Updated via API on {}'.format(now.format());
    return dicts


def getMaxID(table, id_field):
    print('get max ID for table {} col {}'.format(table, id_field))
    query = '''
        SELECT MAX({}) AS max_id FROM {}
    '''.format(id_field, table)
    print(query)
    max_id = kitsutil.data_as_dict(kits_creds, query)
    return int(max_id[0]['max_id'])


def createInsertQuery(table, row):
    cols = str(tuple([key for key in row])).replace("'","")
    vals = str(tuple([row[key] for key in row]))
    
    return '''
        INSERT INTO {} {}
        VALUES {}
    '''.format(table, cols, vals)


def createUpdateQuery(table, row, where_key):
    mod_row = deepcopy(row)

    where = "{} = {}".format(where_key, row[where_key])
    mod_row.pop(where_key)    

    #  append quotes to string fields
    for field in mod_row:
        if field in fieldmap:
            if (fieldmap[field]['table'] == table
                and fieldmap[field]["type"] == str):
                mod_row[field] = "'{}'".format(mod_row[field])

    return '''
        UPDATE {}
        SET {}
        WHERE {};
    '''.format(table, ', '.join('{}={}'.format(
            key, mod_row[key]) for key in mod_row), where
        )


def createMatchQuery(table, return_key, match_key, match_val):
    return '''
        SELECT {}
        FROM {}
        WHERE {} = {}
    '''.format(return_key, table, match_key, match_val)


def createDeleteQuery(table, match_key, match_val):
    return'''
    DELETE FROM {}
    WHERE {} = {}
    '''.format(table, match_key, match_val)


def main(date_time):
    print('starting stuff now')
    
    # get knack data
    kn = knackpy.Knack(
        scene=knack_scene,
        view=knack_view,
        ref_obj=['object_53', 'object_11'],
        app_id=KNACK_CREDENTIALS[app_name]['app_id'],
        api_key=KNACK_CREDENTIALS[app_name]['api_key']
    )

    field_names = kn.fieldnames
    kn.data = datautil.filter_by_key_exists(kn.data, primary_key_knack)
    fieldmap_knack_kits = {
        fieldmap[x]['knack_id'] : x for x in fieldmap.keys()
        if fieldmap[x]['knack_id'] != None
    }

    #  remove entries that do not have filter key 
    for key in filters.keys():  
        knack_data_filtered = datautil.filter_by_key_exists(kn.data, key)

    #  remove entries that do not have filter key/value
    for key in filters.keys():
        knack_data_filtered = datautil.filter_by_val(
            knack_data_filtered, key,
            filters[key]
        )
        
    #  replace knack fieldames with kits fieldnames
    knack_data_repl = datautil.replace_keys(
        knack_data_filtered,
        fieldmap_knack_kits
    )
    
    #  drop record keys that are not in fieldmap
    knack_data_repl = datautil.reduce_to_keys(
        knack_data_repl,
        fieldmap_knack_kits.values()
    )

    knack_data_def = setDefaults(knack_data_repl, fieldmap)
    knack_data_repl = createCAMCOMMENT(knack_data_repl)
    
    #  get kits data
    camera_query = createCameraQuery(kits_table_camera)
    kits_data = kitsutil.data_as_dict(kits_creds, camera_query)
    kits_data_conv = convert_data(kits_data, fieldmap)

    #  compile list of keys to compare and run change detection
    compare_keys = [key for key in fieldmap.keys() if fieldmap[key]['detect_changes'] ]
    data_cd = datautil.detect_changes(kits_data_conv, knack_data_repl, 'CAMNUMBER', keys=compare_keys)
    
    #  insert new records in asset, geo, and webconfig tables
    if data_cd['new']:
        logging.info('new: {}'.format( len(data_cd['new']) ))    
        max_cam_id = getMaxID(kits_table_camera, 'CAMID')
        data_cd['new'] = map_bools(data_cd['new'])
        for record in data_cd['new']:
            time.sleep(1) #  connection will fail if queried are pushed too frequently
            #  insert camera query
            max_cam_id += 1
            record['CAMID'] = max_cam_id
            query_camera = createInsertQuery(kits_table_camera, record)

            #  insert geometry query
            record_geom = {} 
            geometry = "geometry::Point({}, {}, 4326)".format(record['LONGITUDE'], record['LATITUDE'])
            record_geom['GeometryItem'] = geometry
            record_geom['CamID'] = max_cam_id
            query_geom = createInsertQuery(kits_table_geom, record_geom)
            query_geom = query_geom.replace("'", "")  #  strip single quotes from geometry value

            #  insert webconfig query
            record_web = {}
            record_web['WebType'] = 2
            record_web['WebComments'] = ''
            record_web['WebID'] = max_cam_id
            record_web['WebURL'] = 'http://{}'.format(record['VIDEOIP'])
            query_web = createInsertQuery(kits_table_web, record_web)
            
            #  execute queries
            insert_results = kitsutil.insert_multi_table(kits_creds, [query_camera, query_geom, query_web])
    
    if data_cd['change']:
        print(len(data_cd['change']))
        data_cd['change'] = map_bools(data_cd['change'])
        
        logging.info('change: {}'.format( len(data_cd['change']) ))
        for record in data_cd['change']:
            time.sleep(1) #  connection will fail if queried are pushed too frequently
            # fetch camid field, which relates camera, geometry, and webconfig table records
            match_query = createMatchQuery(kits_table_camera, 'CAMID', 'CAMNUMBER', record['CAMNUMBER'])
            match_id = kitsutil.data_as_dict(kits_creds, match_query)
            match_id = int(match_id[0]['CAMID'])

            #  update camera query
            query_camera = createUpdateQuery(kits_table_camera, record, 'CAMNUMBER')

            #  update geometry query
            record_geom = {}
            geometry = "geometry::Point({}, {}, 4326)".format(record['LONGITUDE'], record['LATITUDE'])
            record_geom['GeometryItem'] = geometry
            record_geom['CamID'] = match_id
            query_geom = createUpdateQuery(kits_table_geom, record_geom, 'CamID')

            #  update webconfig query
            record_web = {}
            record_web['WebType'] = 2
            record_web['WebID'] = match_id
            record_web['WebURL'] = 'http://{}'.format(record['VIDEOIP'])
            query_web = createUpdateQuery(kits_table_web, record_web, 'WebID')
            #  execute queries
            insert_results = kitsutil.insert_multi_table(kits_creds, [query_camera, query_geom, query_web])
            
    if data_cd['delete']:
        logging.info('delete: {}'.format( len(data_cd['delete']) ))
        for record in data_cd['delete']:
            time.sleep(1) #  connection will fail if queried are pushed too frequently
            # fetch camid field, which relates camera, geometry, and webconfig table records
            match_query = createMatchQuery(kits_table_camera, 'CAMID', 'CAMNUMBER', record['CAMNUMBER'])
            match_id = kitsutil.data_as_dict(kits_creds, match_query)
            match_id = int(match_id[0]['CAMID'])

            #  update camera query
            query_camera = createDeleteQuery(kits_table_camera, 'CAMID', match_id)

            #  update geometry query
            query_geo = createDeleteQuery(kits_table_geom, 'CamID', match_id)

            #  update webconfig query
            query_web = createDeleteQuery(kits_table_web, 'WebID', match_id)
            #  execute queries
            pdb.set_trace()            
            insert_results = kitsutil.insert_multi_table(kits_creds, [query_camera, query_geo, query_web])

    if data_cd['no_change']:
        logging.info('no_change: {}'.format( len(data_cd['no_change']) ))

    logging.info('END AT {}'.format(arrow.now().format()))

if __name__ == '__main__':
    now = arrow.now()

    #  init logging 
    #  with one logfile per dataset per day
    script = os.path.basename(__file__).replace('.py', '.log')
    logfile = f'{LOG_DIRECTORY}/{script}'
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START AT {}'.format(arrow.now().format()))
    
    try:
        main(now)

    except Exception as e:
        emailutil.send_email(ALERTS_DISTRIBUTION, 'KITS CAMERA SYNC FAILURE', str(e), EMAIL['user'], EMAIL['password'])
        logging.warning(str(e))
        print(e)
        raise e