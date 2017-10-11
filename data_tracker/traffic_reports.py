'''
Parse COA traffic report feed and upload to Knack database.
(A different script publishes the information to the COA pubic data portal).

This feed is generated by some CTM black magic that
extracts incident data from the public safety CAD databse and publishes
it in the form of an RSS feed and cold fusion-powered HTML table.
See: http://www.ci.austin.tx.us/qact/qact_rss.cfm

***Requires Python 3
'''


#   todo:

#   as it turns out: the incident ids (which appear to be hashes) are not unique!!
#   so we need to generate our own unique hashes from these incidents
import logging
import os
import pdb

import arrow
import feedparser
import knackpy

import _setpath
from config.secrets import *
from config.traffic_report_fields import *
from util import emailutil
from util import datautil
from util import agolutil

#  init logging 
now = arrow.now()
now_s = now.format('YYYY_MM_DD')
logfile = '{}/traffic_reports{}.log'.format(LOG_DIRECTORY, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(str(now)))

#  config
feed_url = 'http://www.ci.austin.tx.us/qact/qact_rss.cfm'
primary_key = 'TRAFFIC_REPORT_ID'
status_key = 'TRAFFIC_REPORT_STATUS'
date_field = 'PUBLISHED_DATE'
knack_obj = 'object_121'
knack_scene = 'scene_514'
knack_view = 'view_1624'
id_field_raw = 'field_1826'
knack_status_field = 'field_1843'

knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
agol_creds = AGOL_CREDENTIALS


def get_filter(field_id, field_val):

    return [
            {
               'field' : field_id,
               'operator' : 'is',
               'value' : field_val
            }
        ]


def getRecords():
     #  get "ACTIVE" traffic report records from knack
    filter_current = get_filter(knack_status_field, 'ACTIVE')

    kn = knackpy.Knack(
        view=knack_view,
        scene=knack_scene,
        app_id=knack_creds['app_id'],
        filters=filter_current,
        rows_per_page=1000,
        page_limit=100
    )

    if not kn.data:
        #  if there are no "old" active records
        #  we need some data to get schema
        #  so fetch one record from source table
        kn = knackpy.Knack(
            view=knack_view,
            scene=knack_scene,
            app_id=knack_creds['app_id'],
            page_limit=1,
            rows_per_page=1
        )

        #  we don't want the record, just the schema, so clear kn.data
        kn.data = []

    #  manually insert field metadata into Knackpy object
    #  because field metadata is not avaialble in public views
    #  and we use a public view to reduce the # of private API calls
    #  of which we have a daily limit
    kn.fields = TRAFFIC_REPORT_META
    kn.make_field_map()

    return kn

def parseFeed(feed):
    '''
    extract feed data by applying some unfortunate hardcoded parsing
    logic to feed entries
    '''
    records = []
    for entry in feed.entries:
        record = handleRecord(entry)
        records.append(record)
    return records


def getTimestamp(datestr):
    #  returns a naive millsecond timestamp in local time
    #  which is good because Knack needs a local timestamp
    return arrow.get(datestr).timestamp * 1000

    
def has_match(dicts, val, key):
    #  find the first dict in a list of dicts that matches a key/value
    for record in dicts:
        if record[key] == val:
            return True
    return False
                
def parseTitle(title):
    #  parse a the feed "title" element
    #  assume feed will never have Euro sign (it is non-ascii)
    title = title.replace('    ', '€')
    #  remove remaining whitespace clumps like so: 
    title = " ".join(title.split())
    #  split into list on
    title = title.split('€')
    #  remove empty strings reducing to twoelements
    #  first is address, second is issue type, with leading dash (-)
    title = list(filter(None, title))

    issue = title[1].replace('-', '').strip()
    address = title[0].replace('/', '&')
    return address, issue


def parseSummary(summary):
    #  feed summary is pipe-delimitted and gnarly
    summary = summary.split('|')
    summary = [thing.strip() for thing in summary]
    #  return lat/lon elements from summary array in format [x, y]
    return summary[1:3]


def handleRecord(entry):
    #  turn rss feed entry into traffic report dict
    record = {}
    timestamp = getTimestamp(entry.published_parsed)
    #  compose record id from entry identifier (which is not wholly unique)
    #  and publicatin timestamp
    record_id = '{}_{}'.format( str(entry.id), str(timestamp) )
    record[primary_key] = record_id
    record[date_field] = timestamp
    title = entry.title
    #  parse title
    title = parseTitle(title)
    record['ADDRESS'] = title[0]
    record['ISSUE_REPORTED'] = title[1]
    #  parse lat/lon
    geocode = parseSummary(entry.summary)
    record['LATITUDE'] =  geocode[0]
    record['LONGITUDE'] =  geocode[1]
    record['LOCATION'] = { 'latitude' : geocode[0], 'longitude' : geocode[1] }
    return record


def main(date_time):
    try:
        #  get existing traffic report records from Knack
        kn = getRecords()
       
        #  get and parse feed
        feed = feedparser.parse(feed_url)
        new_records = parseFeed(feed)
       
        #  convert feed fieldnames to Knack database names
        #  prepping them for upsert 
        new_records = datautil.replace_keys(new_records, kn.field_map)
        primary_key_raw = kn.field_map[primary_key]
        status_key_raw = kn.field_map[status_key]
        date_field_raw = kn.field_map[date_field]


        records_archive = []
        #  look for old records in new data
        for old_rec in kn.data:
            if has_match(
                new_records,
                old_rec[primary_key_raw],
                primary_key_raw
            ):
                continue

            else:
                old_rec[status_key_raw] = 'ARCHIVED'
                records_archive.append(old_rec)

        records_archive = datautil.reduce_to_keys(records_archive, ['id', status_key_raw])

        records_insert = []
        for new_rec in new_records:
            #  compare records in current feed with existing "active" recors
            if has_match(
                kn.data,
                new_rec[primary_key_raw],
                primary_key_raw
            ):
                continue

            else:
                new_rec[status_key_raw] = 'ACTIVE'
                records_insert.append(new_rec)

        count = 0

        for record in records_archive:
            count += 1
            print( 'Updating record {} of {}'.format( count, len(records_archive) ) )
            
            res = knackpy.update_record(
                record,
                knack_obj,
                'id',
                knack_creds['app_id'],
                knack_creds['api_key']
            )

        count = 0
        for record in records_insert:
            count += 1
            print( 'Inserting record {} of {}'.format( count, len(records_insert) ) )

            res = knackpy.insert_record(
                record,
                knack_obj,
                knack_creds['app_id'],
                knack_creds['api_key']
            )

        return 'Done.'

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        emailutil.send_email(ALERTS_DISTRIBUTION, 'Traffic Report Process Failure', str(e), EMAIL['user'], EMAIL['password'])
        raise e


results = main(now)
logging.info('END AT {}'.format(str( arrow.now().timestamp) ))

print(results)



