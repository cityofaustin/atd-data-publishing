# Copy CCTV records from Data Tracker to KITS traffic management system.

# Attributes:
#     KITS_CONFIG.get("app_name") (str): Description
#     fieldmap (TYPE): Description
#     KITS_CONFIG["filters"] (TYPE): Description
#     KITS_CREDENTIALS (TYPE): Description
#     KITS_CONFIG.get("kits_table_camera") (str): Description
#     kits_table_geom (str): Description
#     KITS_CONFIG.get("kits_table_web") (str): Description
#     knack_creds (TYPE): Description
#     knack_objects (list): Description
#     KITS_CONFIG.get("knack_scene") (str): Description
#     KITS_CONFIG.get("knack_view") (str): Description
#     max_cam_id (int): Description
#     KITS_CONFIG.get("primary_key_knack") (str): Description


from copy import deepcopy
import os
import pdb
import sys
import time
import traceback

import arrow
import knackpy

import _setpath
from config.kits.config import *
from config.secrets import *

import datautil
import emailutil
import jobutil
import kitsutil
import logutil


fieldmap = KITS_CONFIG["fieldmap"]


def set_technology(dicts):
    for cam in dicts:
        if cam["CAMERA_MFG"] == "Advidia":
            cam["TECHNOLOGY"] = 16
        else:
            cam["TECHNOLOGY"] = None
    return dicts


def map_bools(dicts):
    """Summary
    
    Args:
        dicts (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    #  convert boolean values to 1/0 for SQL compatibility
    for record in dicts:
        for key in record.keys():
            try:
                if fieldmap[key]["type"] == bool:
                    record[key] = int(record[key])
            except KeyError:
                continue

    return dicts


def create_camera_query(table_name):
    """Summary
    
    Args:
        table_name (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return "SELECT * FROM {}".format(table_name)


def convert_data(data, fieldmap):
    """Summary
    
    Args:
        data (TYPE): Description
        fieldmap (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    new_data = []

    for record in data:
        new_record = {
            fieldname: fieldmap[fieldname]["type"](record[fieldname])
            for fieldname in record.keys()
            if fieldname in fieldmap and fieldname in record.keys()
        }

        new_data.append(new_record)

    return new_data


def set_defaults(dicts, fieldmap):
    """Summary
    
    Args:
        dicts (TYPE): Description
        fieldmap (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    for row in dicts:
        for field in fieldmap.keys():

            if (
                field not in row
                and fieldmap[field]["default"] != None
                and fieldmap[field]["table"] == KITS_CONFIG.get("kits_table_camera")
            ):

                row[field] = fieldmap[field]["default"]

    return dicts


def create_cam_comment(dicts):
    """Summary
    
    Args:
        dicts (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    for row in dicts:
        row["CAMCOMMENT"] = "Updated via API on {}".format(arrow.now().format())
    return dicts


def get_max_id(table, id_field):
    """Summary
    
    Args:
        table (TYPE): Description
        id_field (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    print("get max ID for table {} col {}".format(table, id_field))
    query = """
        SELECT MAX({}) AS max_id FROM {}
    """.format(
        id_field, table
    )
    print(query)
    max_id = kitsutil.data_as_dict(KITS_CREDENTIALS, query)
    return int(max_id[0]["max_id"])


def create_insert_query(table, row):
    """Summary
    
    Args:
        table (TYPE): Description
        row (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    cols = str(tuple([key for key in row])).replace("'", "")
    vals = str(tuple([row[key] for key in row]))

    return """
        INSERT INTO {} {}
        VALUES {}
    """.format(
        table, cols, vals
    )


def create_update_query(table, row, where_key):
    """Summary
    
    Args:
        table (TYPE): Description
        row (TYPE): Description
        where_key (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    mod_row = deepcopy(row)

    where = "{} = {}".format(where_key, row[where_key])
    mod_row.pop(where_key)

    #  append quotes to string fields
    for field in mod_row:
        if field in fieldmap:
            if fieldmap[field]["table"] == table and fieldmap[field]["type"] == str:
                mod_row[field] = "'{}'".format(mod_row[field])

    return """
        UPDATE {}
        SET {}
        WHERE {};
    """.format(
        table, ", ".join("{}={}".format(key, mod_row[key]) for key in mod_row), where
    )


def create_match_query(table, return_key, match_key, match_val):
    """Summary
    
    Args:
        table (TYPE): Description
        return_key (TYPE): Description
        match_key (TYPE): Description
        match_val (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return """
        SELECT {}
        FROM {}
        WHERE {} = {}
    """.format(
        return_key, table, match_key, match_val
    )


def create_delete_query(table, match_key, match_val):
    """Summary
    
    Args:
        table (TYPE): Description
        match_key (TYPE): Description
        match_val (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return """
    DELETE FROM {}
    WHERE {} = {}
    """.format(
        table, match_key, match_val
    )


def main():
    """Summary
    
    Args:
        job (TYPE): Description
        **kwargs: Description
    
    Returns:
        TYPE: Description
    """
    kn = knackpy.Knack(
        scene=KITS_CONFIG.get("knack_scene"),
        view=KITS_CONFIG.get("knack_view"),
        ref_obj=["object_53", "object_11"],
        app_id=KNACK_CREDENTIALS[KITS_CONFIG.get("app_name")]["app_id"],
        api_key=KNACK_CREDENTIALS[KITS_CONFIG.get("app_name")]["api_key"],
    )

    field_names = kn.fieldnames
    kn.data = datautil.filter_by_key_exists(
        kn.data, KITS_CONFIG.get("primary_key_knack")
    )
    fieldmap_knack_kits = {
        fieldmap[x]["knack_id"]: x
        for x in fieldmap.keys()
        if fieldmap[x]["knack_id"] != None
    }

    for key in KITS_CONFIG["filters"].keys():
        knack_data_filtered = datautil.filter_by_key_exists(kn.data, key)

    for key in KITS_CONFIG["filters"].keys():
        knack_data_filtered = datautil.filter_by_val(
            knack_data_filtered, key, KITS_CONFIG["filters"][key]
        )

    knack_data_filtered = set_technology(knack_data_filtered)

    knack_data_repl = datautil.replace_keys(knack_data_filtered, fieldmap_knack_kits)

    knack_data_repl = datautil.reduce_to_keys(
        knack_data_repl, fieldmap_knack_kits.values()
    )

    knack_data_def = set_defaults(knack_data_repl, fieldmap)
    knack_data_repl = create_cam_comment(knack_data_repl)

    camera_query = create_camera_query(KITS_CONFIG.get("kits_table_camera"))
    kits_data = kitsutil.data_as_dict(KITS_CREDENTIALS, camera_query)
    kits_data_conv = convert_data(kits_data, fieldmap)

    compare_keys = [key for key in fieldmap.keys() if fieldmap[key]["detect_changes"]]
    data_cd = datautil.detect_changes(
        kits_data_conv, knack_data_repl, "CAMNUMBER", keys=compare_keys
    )

    if data_cd["new"]:
        # logger.info('new: {}'.format( len(data_cd['new']) ))

        max_cam_id = get_max_id(KITS_CONFIG.get("kits_table_camera"), "CAMID")
        data_cd["new"] = map_bools(data_cd["new"])

        for record in data_cd["new"]:
            time.sleep(1)  #  connection will fail if queries are pushed too frequently

            max_cam_id += 1
            record["CAMID"] = max_cam_id
            query_camera = create_insert_query(
                KITS_CONFIG.get("kits_table_camera"), record
            )

            record_geom = {}
            geometry = "geometry::Point({}, {}, 4326)".format(
                record["LONGITUDE"], record["LATITUDE"]
            )
            record_geom["GeometryItem"] = geometry
            record_geom["CamID"] = max_cam_id
            query_geom = create_insert_query(
                KITS_CONFIG.get("kits_table_geom"), record_geom
            )
            query_geom = query_geom.replace(
                "'", ""
            )  #  strip single quotes from geometry value

            record_web = {}
            record_web["WebType"] = 2
            record_web["WebComments"] = ""
            record_web["WebID"] = max_cam_id
            record_web["WebURL"] = "http://{}".format(record["VIDEOIP"])
            query_web = create_insert_query(
                KITS_CONFIG.get("kits_table_web"), record_web
            )

            insert_results = kitsutil.insert_multi_table(
                KITS_CREDENTIALS, [query_camera, query_geom, query_web]
            )

    if data_cd["change"]:

        data_cd["change"] = map_bools(data_cd["change"])

        # logger.info('change: {}'.format( len(data_cd['change']) ))

        for record in data_cd["change"]:
            time.sleep(1)  #  connection will fail if queried are pushed too frequently
            # fetch camid field, which relates camera, geometry, and webconfig table records
            match_query = create_match_query(
                KITS_CONFIG.get("kits_table_camera"),
                "CAMID",
                "CAMNUMBER",
                record["CAMNUMBER"],
            )
            match_id = kitsutil.data_as_dict(KITS_CREDENTIALS, match_query)
            match_id = int(match_id[0]["CAMID"])

            query_camera = create_update_query(
                KITS_CONFIG.get("kits_table_camera"), record, "CAMNUMBER"
            )

            record_geom = {}
            geometry = "geometry::Point({}, {}, 4326)".format(
                record["LONGITUDE"], record["LATITUDE"]
            )
            record_geom["GeometryItem"] = geometry
            record_geom["CamID"] = match_id
            query_geom = create_update_query(
                KITS_CONFIG.get("kits_table_geom"), record_geom, "CamID"
            )

            record_web = {}
            record_web["WebType"] = 2
            record_web["WebID"] = match_id
            record_web["WebURL"] = "http://{}".format(record["VIDEOIP"])
            query_web = create_update_query(
                KITS_CONFIG.get("kits_table_web"), record_web, "WebID"
            )

            insert_results = kitsutil.insert_multi_table(
                KITS_CREDENTIALS, [query_camera, query_geom, query_web]
            )

    if data_cd["delete"]:

        # logger.info('delete: {}'.format( len(data_cd['delete']) ))

        for record in data_cd["delete"]:
            time.sleep(1)  #  connection will fail if queried are pushed too frequently
            # fetch camid field, which relates camera, geometry, and webconfig table records
            match_query = create_match_query(
                KITS_CONFIG.get("kits_table_camera"),
                "CAMID",
                "CAMNUMBER",
                record["CAMNUMBER"],
            )
            match_id = kitsutil.data_as_dict(KITS_CREDENTIALS, match_query)
            match_id = int(match_id[0]["CAMID"])

            query_camera = create_delete_query(
                KITS_CONFIG.get("kits_table_camera"), "CAMID", match_id
            )

            query_geo = create_delete_query(
                KITS_CONFIG.get("kits_table_geom"), "CamID", match_id
            )

            query_web = create_delete_query(
                KITS_CONFIG.get("kits_table_web"), "WebID", match_id
            )

            insert_results = kitsutil.insert_multi_table(
                KITS_CREDENTIALS, [query_camera, query_geo, query_web]
            )

    # if data_cd['no_change']:
    # logger.info('no_change: {}'.format( len(data_cd['no_change']) ))

    # logger.info('END AT {}'.format( arrow.now().format() ))

    results = {"total": 0}

    for result in ["new", "change", "no_change", "delete"]:
        results["total"] += len(data_cd[result])
        results[result] = len(data_cd[result])

    return results.get("change")


if __name__ == "__main__":
    main()
