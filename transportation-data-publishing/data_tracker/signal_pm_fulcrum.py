"""Summary

Attributes:
    form_id (str): Description
    key (TYPE): Description
    pgrest (TYPE): Description
"""
import pandas as pd
import numpy as np
import knackpy
import fulcrum as fc
import requests
import pdb
import json
from datetime import datetime, timedelta
# import requests

# import credentials
import _setpath
from config.secrets import *

from tdutils.pgrestutil import Postgrest

form_id = "44359e32-1a7f-41bd-b53e-3ebc039bd21a"
key = FULCRUM.get("api_key")

# create postgrest instance
pgrest = Postgrest(
    "http://transportation-data-test.austintexas.io/signal_pms",
    auth=JOB_DB_API_TOKEN_TEST,
)

def get_postgre_records():
	"""Summary
	
	Returns:
	    TYPE: Description
	"""
	postgre_records = pgrest.select("")
	
	return postgre_records


def get_knack_pm_records():
	"""Summary
	
	Returns:
	    TYPE: Description
	"""
	# pick only pm_records with fulcrum id

    signals_pms_fulcrum = knackpy.Knack(
        scene = "scene_952",
        view = "view_2405",
        ref_obj = ["object_84"],
        api_key = KNACK_CREDENTIALS["data_tracker_test"]["api_key"],
        app_id = KNACK_CREDENTIALS["data_tracker_test"]["app_id"],
        timeout = 30,
    )

    return signals_pms_fulcrum.data


def get_signals_knack_id():
    """Summary
    
    Returns:
        TYPE: Description
    """
    signals_knack_id_dict = knackpy.Knack(
        scene="scene_73",
        view="view_197",
        ref_obj=["object_12"],
        api_key=KNACK_CREDENTIALS["data_tracker_test"]["api_key"],
        app_id=KNACK_CREDENTIALS["data_tracker_test"]["app_id"],
        timeout=30,
    )

    return signals_knack_id_dict.data



def get_last_run():
	"""Summary
	"""



	pass

def prepare_payloads():
	"""Summary
	"""



	pass

def update_pms():
	"""Summary
	"""
	pass

def update_signals_modified_time():
	"""Summary
	"""
	pass

def main():
	"""Summary
	"""
	pass


if __name__ == "__main__":
	# signals_knack_id = get_signals_knack_id()

	# print(signals_knack_id[0])
	pgrest_records = get_postgre_records()
	knack_records = get_knack_pm_records()

	# print(pgrest_records)

	print(knack_records)

