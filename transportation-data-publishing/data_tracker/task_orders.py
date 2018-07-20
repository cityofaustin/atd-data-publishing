'''
Scrap task orders from COA Controller webpage and upload to Data Tracker.
'''
import json
import os
import pdb
import traceback
import sys

import arrow
import knackpy
from bs4 import BeautifulSoup
import requests

import _setpath
from config.knack.config import cfg
from config.secrets import *

from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import logutil

CONFIG = cfg['task_orders']
KNACK_CREDS = KNACK_CREDENTIALS['data_tracker_prod']

def get_html(url):
    form_data = {'DeptNumber' : 2400, 'Search': 'Search', 'TaskOrderName': ''}
    res = requests.post(url, data=form_data)
    res.raise_for_status()
    return res.text


def handle_html(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all('tr')
    
    parsed = []

    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        parsed.append(cols)

    return parsed


def handle_rows(rows, cols=['DEPT', 'TASK_ORDER', 'NAME', 'ACTIVE']):
    handled = []

    for row in rows:
        #  janky check to exclude rows that don't match expected schema
        if len(row) == 4:
            handled.append( dict(zip(cols, row)) )

    return handled


def compare(new_rows, existing_rows, key='TASK_ORDER'):
    existing_ids = [str(row[key]) for row in existing_rows]
    return [row for row in new_rows if str(row[key]) not in existing_ids]


def main(job, **kwargs):
    html = get_html(TASK_ORDERS_ENDPOINT)
    data = handle_html(html)
    rows = handle_rows(data)

    kn = knackpy.Knack(
        scene=CONFIG['scene'],
        view=CONFIG['view'],
        ref_obj=CONFIG['ref_obj'],
        app_id=KNACK_CREDS['app_id'],
        api_key=KNACK_CREDS['api_key']
    )

    new_rows = compare(rows, kn.data)

    new_rows = datautil.replace_keys(new_rows, kn.field_map)

    for record in new_rows:

        res = knackpy.record(
            record,
            obj_key=CONFIG['ref_obj'][0],
            app_id=KNACK_CREDS['app_id'],
            api_key=KNACK_CREDS['api_key'],
            method='create',
        )

    return len(new_rows)


if __name__=='__main__':
    
    try:
        script_name = os.path.basename(__file__).replace('.py', '')
        logfile = f'{LOG_DIRECTORY}/{script_name}.log'

        logger = logutil.timed_rotating_log(logfile)
        logger.info('START AT {}'.format( arrow.now() ))

        CONFIG = cfg['task_orders']
        KNACK_CREDS = KNACK_CREDENTIALS['data_tracker_prod']
        
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='kits',
            destination='knack',
            auth=JOB_DB_API_TOKEN)

        job.start()

        results = main()

        job.result('success', records_processed=results)

        logger.info('END AT: {}'.format( arrow.now() ))
    

    except Exception as e:        
        error_text = traceback.format_exc()
        logger.error(error_text)

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'DATA PROCESSING ALERT: Task Order Update Failure',
            error_text,
            EMAIL['user'],
            EMAIL['password']
        )

        job.result('error', message=str(e) )

        raise e















