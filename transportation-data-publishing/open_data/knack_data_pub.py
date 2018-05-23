'''
Extract data from Knack database and publish to Socrata, ArcGIS Online, 
or CSV.

By default, destination data is incrementally updated based on a
modified date field defined in the configuration file. Alternatively use
--replace to truncate/replace the entire dataset. CSV output is always handled
with --replace.

#TODO
- filter fetch locations by source data
'''
import argparse
from copy import deepcopy
import os
import pdb
import traceback

import arrow
import knackpy

import _setpath
from config.knack.config import cfg as CFG
from config.secrets import *
from util import agolutil
from util import argutil
from util import datautil
from util import emailutil
from util import jobutil
from util import knackutil
from util import logutil
from util import socratautil



def socrata_pub(records, cfg, date_fields=None):
    if cfg.get('location_fields'):            
        lat_field = cfg['location_fields']['lat'].lower()
        lon_field = cfg['location_fields']['lon'].lower()
        location_field = cfg['location_fields']['location_field'].lower()
    else:
        lat_field = None
        lon_field = None
        location_field = None

    return socratautil.Soda(
        auth=SOCRATA_CREDENTIALS,
        records=records,
        resource=cfg['socrata_resource_id'],
        date_fields=date_fields,
        lat_field=lat_field,
        lon_field=lon_field,
        location_field=location_field,
        replace=args.replace)


def agol_pub(records, cfg):
    '''
    Upsert or replace records on arcgis online features service
    '''

    if cfg.get('location_fields'):                    
        lat_field = cfg['location_fields']['lat']
        lon_field = cfg['location_fields']['lon']
    else:
        lat_field = None
        lon_field = None
                
    layer = agolutil.get_layer(auth=AGOL_CREDENTIALS, 
                                service_id=cfg['service_id'])
    
    if args.replace:
        res = layer.manager.truncate()

        if not res.get('success'):
            raise Exception('AGOL truncate failed.') 

    else:
        '''
        Delete objects by primary key. ArcGIS api does not currently support
        an upsert method, although the Python api defines one via the
        layer.append method, it is apparently still under development. So our
        "upsert" consists of a delete by primary key then add.
        '''
        primary_key = cfg.get('primary_key')

        delete_ids = [record[primary_key] for record in records]
        delete_ids = ', '.join(str(x) for x in delete_ids)

        #  generate a SQL-like where statement to identify records for deletion
        where = '{} in ({})'.format(primary_key, delete_ids)

        res = layer.delete_features(where=where)
        agolutil.handle_response(res)

    for i in range(0, len(records), 1000):
        '''Chunk records like a boss'''
        print(i)
        adds = agolutil.feature_collection(
            records[i:i + 1000],
            lat_field=lat_field,
            lon_field=lon_field)

        res = layer.edit_features(adds=adds)
        agolutil.handle_response(res)

    return True


def write_csv(knackpy_instance, cfg):
    if cfg.get('csv_separator'):
        sep = cfg['csv_separator']
    else:
        sep = ','

    file_name = '{}/{}.csv'.format(FME_DIRECTORY, args.dataset)
    knackpy_instance.to_csv(file_name, delimiter=sep)
    
    return True


def knackpy_wrapper(cfg, auth, filters=None):
    return knackpy.Knack(
        obj=cfg['obj'],
        scene=cfg['scene'],
        view=cfg['view'],
        ref_obj=cfg['ref_obj'],
        app_id=auth['app_id'],
        api_key=auth['api_key'],
        filters=filters,
        page_limit=10000
    )


def get_multi_source(cfg, auth, last_run_date):
    '''
    Return a single knackpy dataset instance from multiple knack sources. Developed
    specifically for merging traffic and phb signal requests into a single dataset.

    See note in main() about why we filter by date twice for each knackpy instance.
    '''
    kn = None

    for source_cfg in cfg['sources']:
        
        filters = knackutil.date_filter_on_or_after(
            last_run_date,
            source_cfg['modified_date_field_id'])

        last_run_timestamp = arrow.get(last_run_date).timestamp * 1000

        if not kn:
            kn = knackpy_wrapper(source_cfg, auth, filters)

            if kn.data:
                kn.data = filter_by_date(kn.data,
                    source_cfg['modified_date_field'], last_run_timestamp)
            else:
                #  Replace None with empty list
                kn.data = []

        else:
            kn_temp = knackpy_wrapper(source_cfg, auth, filters)

            if kn_temp.data:
                kn_temp.data = filter_by_date(kn_temp.data,
                    source_cfg['modified_date_field'], last_run_timestamp)

            else:
                kn_temp.data = []
            
            kn.data = kn.data + kn_temp.data
            kn.fields.update(kn_temp.fields)

    return kn


def filter_by_date(data, date_field, compare_date):
    '''
    Date field and compare date should be unix timestamps with mills
    '''
    return [record for record in data if record[date_field] >= compare_date]


def main(cfg, auth, job, args):
    
    last_run_date = job.most_recent()

    if not last_run_date or args.replace or job.destination == 'csv':
        # replace dataset by setting the last run date to a long, long time ago
        last_run_date = '1/1/1900'

    '''
    We include a filter in our API call to limit to records which have
    been modified on or after the date the last time this job ran
    successfully. The Knack API supports filter requests by date only
    (not time), so we must apply an additional filter on the data after
    we receive it.
    '''


    if cfg.get('multi_source'):
        kn = get_multi_source(cfg, auth, last_run_date)

    else:
        
        filters = knackutil.date_filter_on_or_after(
            last_run_date,
            cfg['modified_date_field_id'])


        kn = knackpy_wrapper(cfg, auth, filters=filters)
    
        if kn.data:
            # Filter data for records that have been modifed after the last 
            # job run (see comment above)
            last_run_timestamp = arrow.get(last_run_date).timestamp * 1000
            kn.data = filter_by_date(kn.data, cfg['modified_date_field'], last_run_timestamp)

    if not kn.data:
        return 0

    if cfg.get('fetch_locations'):
        '''
        Optionally fetch location data from another knack view and merge with
        primary dataset. We access the base CFG object to pull request info
        from the 'locations' config.
        '''
        locations = knackpy_wrapper(CFG['locations'], auth)

        lat_field = CFG['locations']['location_fields']['lat']
        lon_field = CFG['locations']['location_fields']['lon']

        kn.data = datautil.merge_dicts(
            kn.data,
            locations.data,
            cfg['location_join_field'],
            [lat_field, lon_field]
        )

    date_fields = [kn.fields[f]['label'] for f in kn.fields if kn.fields[f]['type'] in ['date_time', 'date']]

    if job.destination == 'socrata':
        pub = socrata_pub(kn.data, cfg, date_fields=date_fields)

    if job.destination == 'agol':
        pub = agol_pub(kn.data, cfg)

    if job.destination == 'csv':
        write_csv(kn, cfg)
        
    logger.info('END AT {}'.format( arrow.now() ))
    
    return len(kn.data)


def cli_args():

    parser = argutil.get_parser(
        'knack_data_pub.py',
        'Publish Knack data to Socrata and ArcGIS Online',
        'dataset',
        'app_name',
        '--destination',
        '--replace'
    )
    
    args = parser.parse_args()
    
    return args


if __name__ == '__main__':
    script_name = os.path.basename(__file__).replace('.py', '')
    logfile = f'{LOG_DIRECTORY}/{script_name}.log'
    
    logger = logutil.timed_rotating_log(logfile)
    logger.info('START AT {}'.format( arrow.now() ))

    args = cli_args()
    logger.info( 'args: {}'.format( str(args) ))

    cfg = CFG[args.dataset]

    for dest in args.destination:    
        '''
        Knack data pub is a special case in which multiple "jobs" are kicked off
        within a single script. For each specified destination, we query knack
        based on the last_run_date and publish data accordingly. The result is
        that each time the script runs, the source db (knack) will likely be sent
        multiple requests for the same data, because each job will probably share
        the same last run date (but we don't want to assume so). This isn't such
        a big deal for incremental updates, but when the --replace option is used,
        all the source data will be downloaded in it's entirety for each
        destination!

        The underlying issue here is that this kind of ETL process should really
        use a staging database, so that source data is extracted once and 
        individual process run separately to publish from the staging DB to the
        various destination datasets. That's a task for another day, hopefully
        soon...
        '''
        try:
            script_id = '{}_{}_{}_{}'.format(
                script_name,
                args.dataset,
                'knack',
                dest)

            job = jobutil.Job(
                name=script_id,
                url=JOB_DB_API_URL,
                source='knack',
                destination=dest,
                auth=JOB_DB_API_TOKEN)

            job.start()

            results = main(cfg, KNACK_CREDENTIALS[args.app_name], job, args)

            job.result(
                'success',
                records_processed=results)
    
        except Exception as e:
            error_text = traceback.format_exc()

            logger.error(error_text)
            
            email_subject = "Knack Data Pub Failure: {}".format(args.dataset)
            
            emailutil.send_email(
                ALERTS_DISTRIBUTION,
                email_subject,
                error_text,
                EMAIL['user'],
                EMAIL['password']
            )
            
            job.result('error', message=str(e))

            continue
        

