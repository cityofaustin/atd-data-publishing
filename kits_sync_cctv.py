if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import os
import sys
import pdb
from copy import deepcopy
import logging
import arrow
from config import config
import kits_helpers
import knack_helpers
import data_helpers
import email_helpers
import secrets

log_directory = secrets.LOG_DIRECTORY

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
        "knack_id" : "LOCATION_NAME",
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
        "knack_id" : "LATITUDE",
        "type" : float,
        "detect_changes" : False,
        "table" : kits_table_camera
    },
    "LONGITUDE" : {
        "knack_id" : "LONGITUDE",
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
knack_view = '395'
knack_scene = '144'
knack_objects = ['object_53', 'object_11']
knack_creds = secrets.KNACK_CREDENTIALS
kits_creds = secrets.KITS_CREDENTIALS
max_cam_id = 0


filters = {
   'CAMERA_STATUS' : ['TURNED_ON'],
   'CAMERA_MFG' : ['Axis', 'Sarix', 'Spectra Enhanced']
}


def createCameraQuery(table_name):
    return "SELECT * FROM {}".format(table_name)


def convert_data(data, fieldmap):
    new_data = []

    for record in data:
        new_record = { fieldname : fieldmap[fieldname]['type'](record[fieldname]) for fieldname in record.keys() if fieldname in fieldmap and record[fieldname] }
        new_data.append(new_record)
        
    return new_data


def setDefaults(list_of_dicts, fieldmap):
    for row in list_of_dicts:
        for field in fieldmap.keys():
            if field not in row and fieldmap[field]['default'] != None and fieldmap[field]['table'] == kits_table_camera:
                row[field] = fieldmap[field]['default']

    return list_of_dicts


def createCAMCOMMENT(list_of_dicts):
    for row in list_of_dicts:
        row['CAMCOMMENT'] = 'Updated via API on {}'.format(now.format());
    return list_of_dicts


def getMaxID(table, id_field):
    print('get max ID for table {} col {}'.format(table, id_field))
    query = '''
        SELECT MAX({}) AS max_id FROM {}
    '''.format(id_field, table)
    print(query)
    max_id = kits_helpers.data_as_dict(kits_creds, query)
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
            if fieldmap[field]['table'] == table and fieldmap[field]["type"] == str:
                mod_row[field] = "'{}'".format(mod_row[field])

    return '''
        UPDATE {}
        SET {}
        WHERE {};
    '''.format(table, ', '.join('{}={}'.format(key, mod_row[key]) for key in mod_row), where)


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
    field_data = knack_helpers.get_fields(knack_objects, knack_creds)
    knack_data = knack_helpers.get_data(knack_scene, knack_view, knack_creds)
    knack_data = knack_helpers.parse_data(knack_data, field_data, convert_to_unix=True)
    field_names = data_helpers.unique_keys(knack_data)
    knack_data = data_helpers.filter_by_key_exists(knack_data, primary_key_knack)
    fieldmap_knack_kits = {fieldmap[x]['knack_id'] : x for x in fieldmap.keys() if fieldmap[x]['knack_id'] != None}
    knack_data_filtered = knack_data

    #  apply filters to knack data, replace keys to match kits, and set default values
    for key in filters.keys():
        knack_data_filtered = data_helpers.filter_by_key_exists(knack_data_filtered, key)

    for key in filters.keys():
        knack_data_filtered = data_helpers.filter_by_key(knack_data_filtered, key, filters[key])

    knack_data_repl = data_helpers.replace_keys(knack_data_filtered, fieldmap_knack_kits, delete_unmatched=True)

    knack_data_def = setDefaults(knack_data_repl, fieldmap)
    knacnk_data_def = createCAMCOMMENT(knack_data_def)

    #  get kits data
    camera_query = createCameraQuery(kits_table_camera)
    kits_data = kits_helpers.data_as_dict(kits_creds, camera_query)
    kits_data_conv = convert_data(kits_data, fieldmap)
    
    #  compile list of keys to compare and run change detection
    compare_keys = [key for key in fieldmap.keys() if fieldmap[key]['detect_changes'] ]
    data_cd = data_helpers.detect_changes(kits_data_conv, knack_data_repl, 'CAMNUMBER', keys=compare_keys)
    
    #  insert new records in asset, geo, and webconfig tables
    if data_cd['new']:
        logging.info('new: {}'.format( len(data_cd['new']) ))    
        max_cam_id = getMaxID(kits_table_camera, 'CAMID')
        
        for record in data_cd['new']:
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
            query_geom = query_geom.replace("'", "")  #  strip quotes from geometry value

            #  insert webconfig query
            record_web = {}
            record_web['WebType'] = 2
            record_web['WebComments'] = ''
            record_web['WebID'] = max_cam_id
            record_web['WebURL'] = 'http://{}'.format(record['VIDEOIP'])
            query_web = createInsertQuery(kits_table_web, record_web)
            
            #  execute queries
            insert_results = kits_helpers.insert_multi_table(kits_creds, [query_camera, query_geom, query_web])
    
    if data_cd['change']:
        logging.info('change: {}'.format( len(data_cd['change']) ))
        for record in data_cd['change']:
            # fetch camid field, which relates camera, geometry, and webconfig table records
            match_query = createMatchQuery(kits_table_camera, 'CAMID', 'CAMNUMBER', record['CAMNUMBER'])
            match_id = kits_helpers.data_as_dict(kits_creds, match_query)
            match_id = int(match_id[0]['CAMID'])

            #  update camera query
            query_camera = createUpdateQuery(kits_table_camera, record, 'CAMNUMBER')

            #  update geometry query
            record_geom = {}
            geometry = "geometry::Point({}, {}, 4326)".format(record['LONGITUDE'], record['LATITUDE'])
            record_geom['GeometryItem'] = geometry
            record_geom['CamID'] = match_id
            query_geo = createUpdateQuery(kits_table_geom, record_geom, 'CamID')

            #  update webconfig query
            record_web = {}
            record_web['WebType'] = 2
            record_web['WebID'] = match_id
            record_web['WebURL'] = 'http://{}'.format(record['VIDEOIP'])
            query_web = createUpdateQuery(kits_table_web, record_web, 'WebID')
            #  execute queries
            insert_results = kits_helpers.insert_multi_table(kits_creds, [query_camera, query_geo, query_web])
            
    if data_cd['delete']:
        logging.info('delete: {}'.format( len(data_cd['delete']) ))
        for record in data_cd['delete']:
            # fetch camid field, which relates camera, geometry, and webconfig table records
            match_query = createMatchQuery(kits_table_camera, 'CAMID', 'CAMNUMBER', record['CAMNUMBER'])
            match_id = kits_helpers.data_as_dict(kits_creds, match_query)
            match_id = int(match_id[0]['CAMID'])

            #  update camera query
            query_camera = createDeleteQuery(kits_table_camera, 'CAMID', match_id)

            #  update geometry query
            query_geo = createDeleteQuery(kits_table_geom, 'CamID', match_id)

            #  update webconfig query
            query_web = createDeleteQuery(kits_table_web, 'WebID', match_id)
            #  execute queries
            insert_results = kits_helpers.insert_multi_table(kits_creds, [query_camera, query_geo, query_web])

    if data_cd['no_change']:
        logging.info('no_change: {}'.format( len(data_cd['no_change']) ))

    logging.info('END AT {}'.format(arrow.now().format()))

if __name__ == '__main__':
    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    #  init logging 
    #  with one logfile per dataset per day
    cur_dir = os.path.dirname(__file__)
    logfile = '{}/kits_sync_cctv_{}.log'.format(log_directory, now_s)
    log_path = os.path.join(cur_dir, logfile)
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logging.info('START AT {}'.format(arrow.now().format()))
    
    try:
        main(now)

    except Exception as e:
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'KITS CAMERA SYNC FAILURE', str(e))
        logging.warning(str(e))
        print(e)
        raise e