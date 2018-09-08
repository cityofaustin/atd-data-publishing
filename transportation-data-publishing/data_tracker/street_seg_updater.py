
# Update Knack street segments with data from
# COA ArcGIS Online Street Segment Feature Service

import pdb

import arrow
import knackpy
from tdutils import agolutil
from tdutils import argutil
from tdutils import datautil
from tdutils import jobutil
from tdutils import knackutil

import _setpath
from config.secrets import *
from config.knack.config import STREET_SEG_UPDATER as config

from tdutils import agolutil
from tdutils import datautil
from tdutils import emailutil
from tdutils import jobutil
from tdutils import knackutil
from tdutils import logutil

config = {
    'modified_date_field_id' : 'field_144',
    'modified_date_field' : 'MODIFIED_DATE',
    'primary_key' : 'SEGMENT_ID_NUMBER',
    'ref_obj' : ['object_7'],
    'scene' : 'scene_424',
    'view' : 'view_1198',
}


def filter_by_date(data, date_field, compare_date):
    '''
    Date field and compare date should be unix timestamps with mills
    '''
    return [record for record in data if record[date_field] >= compare_date]


def main(config, last_run_date):

    if not last_run_date:
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = '1/1/2018'

    filters = knackutil.date_filter_on_or_after(last_run_date,config['modified_date_field_id'])

    '''
    We include a filter in our API call to limit to records which have
    been modified on or after the date the last time this job ran
    successfully. The Knack API supports filter requests by date only
    (not time), so we must apply an additional filter on the data after
    we receive it.
    '''

    args = cli_args()
    app_name = args.app_name
    last_run_date = args.last_run_date

    knack_creds = KNACK_CREDENTIALS[app_name]

    if not last_run_date:
        # replace dataset by setting the last run date to a long, long time ago
        # the arrow package needs a specific date and timeformat
        last_run_date = "1970-01-01"

    filters = knackutil.date_filter_on_or_after(
        last_run_date, config["modified_date_field_id"]
    )
    """
    We include a filter in our API call to limit to records which have
    been modified on or after the date the last time this job ran
    successfully. The Knack API supports filter requests by date only
    (not time), so we must apply an additional filter on the data after
    we receive it.
    """
    kn = knackpy.Knack(
        scene=config['scene'],
        view=config['view'],
        ref_obj=config['ref_obj'],
        app_id=AUTH['app_id'],
        api_key=AUTH['api_key'],
        filters=filters
    )

    if kn.data:
        # Filter data for records that have been modifed after the last 
        # job run (see comment above)
        last_run_timestamp = arrow.get(last_run_date).timestamp * 1000
        kn.data = filter_by_date(kn.data, config['modified_date_field'], last_run_timestamp)

    payload = []
    unmatched_segments = []

    if not kn.data:
        # logger.info('No records to update.')
        return 0

    for street_segment in kn.data:

        token = agolutil.get_token(AGOL_CREDENTIALS)
        features = agolutil.query_atx_street(street_segment[config['primary_key']], token)

        if features.get("features"):
            if len(features["features"]) > 0:
                segment_data = features["features"][0]["attributes"]
            else:
                unmatched_segments.append(street_segment[config['primary_key']])
                continue
        else:
            unmatched_segments.append(street_segment[config['primary_key']])
            continue

        segment_data['id'] = street_segment['id']
        segment_data['MODIFIED_BY'] = 'api-update' 
        payload.append(segment_data)

    payload = datautil.reduce_to_keys(payload, kn.fieldnames)
    payload = datautil.replace_keys(payload, kn.field_map)

    update_response = []
    count = 1

    for record in payload:
        print("updating record {} of {}".format(count, len(payload)))

        #  remove whitespace from janky Esri attributes
        for field in record:
            if type(record[field]) == str:
                record[field] = record[field].strip()

        res = knackpy.record(
            record,
            obj_key=config['ref_obj'][0],
            app_id= AUTH['app_id'],
            api_key=AUTH['api_key'],
            method='update',
        )

        count += 1

        update_response.append(res)

    if len(unmatched_segments) > 0:
        error_text = "Unmatched street segments: {}".format(
            ", ".join(str(x) for x in unmatched_segments)
        )

        raise Exception(error_text)

    return count



if __name__ == '__main__':
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'

    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))


    AUTH = KNACK_CREDENTIALS['data_tracker_prod']


    try:
        
        job = jobutil.Job(
            name=script_name,
            url=JOB_DB_API_URL,
            source='knack',
            destination='knack',
            auth=JOB_DB_API_TOKEN)
        
        job.start()

        last_run_date = job.most_recent()

        results = main(config, last_run_date)

        job.result('success', records_processed=results)

        logger.info('END AT {}'.format( arrow.now() ))
    
    except Exception as e:
        logger.error( str(e) )

        emailutil.send_email(
            ALERTS_DISTRIBUTION,
            'Street Segment Update Failure',
            str(e),
            EMAIL['user'],
            EMAIL['password'])

        job.result('error', message=str(e))
        raise e










