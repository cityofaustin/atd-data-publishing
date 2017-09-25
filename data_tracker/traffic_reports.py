'''
Parse weird COA traffic report feed,
look up geometry, and upload to Knack database.
(A different script publishes the information to the pubic data portal).
'''

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

knack_creds = KNACK_CREDENTIALS['data_tracker_prod']
agol_creds = AGOL_CREDENTIALS
geocoder = 'http://www.austintexas.gov/GIS/REST/Geocode/COA_Street_Locator/GeocodeServer/findAddressCandidates'

def parseTitle(title):
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


def handleGeocode(geocode, record, field_map):
    #  parse geocode and apply field lookup
    if 'candidates' in geocode:
        if geocode['candidates']:
            y = round(geocode['candidates'][0]['location']['y'], 7)
            x = round(geocode['candidates'][0]['location']['x'], 7)
            record[field_map['LOCATION']] = { 'latitude' : y, 'longitude' : x }
            record[field_map['LATITUDE']] = y
            record[field_map['LONGITUDE']] = x
            record[field_map['FOUND_ADDRESS']] = geocode['candidates'][0]['address']
            record[field_map['GEOCODE_SCORE']] = geocode['candidates'][0]['score']
        else:
            return record

    return record


def get_filter(field_id, field_val):

    return [
            {
               'field' : field_id,
               'operator' : 'is',
               'value' : field_val
            }
        ]
    
def has_match(match_list, val, key):
    for rec in match_list:
        print
        if rec[key] == val:
            return True
    return False
                

def handleDateField(struct_time):
    #  return a naive millsecond timestamp in local time
    #  which is good because Knack needs a local timestamp
    return arrow.get(struct_time).timestamp * 1000


def main(date_time):

    try:
        #  get "ACTIVE" records from knack
        filter_current = get_filter('field_1843', 'ACTIVE')

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
            #  so fetch one record from table and overwrite data
            kn = knackpy.Knack(
                view=knack_view,
                scene=knack_scene,
                app_id=knack_creds['app_id'],
                page_limit=1,
                rows_per_page=1
            )

            kn.data = []

        #  we manually insert field metadata into Knackpy object
        #  because field metadata is not avaialble in public views
        #  and we use a public view to reduce the # of private API calls
        #  of which we have a daily limits
        kn.fields = TRAFFIC_REPORT_META
        kn.make_field_map()

        #  get and parse feed
        feed = feedparser.parse(feed_url)
        
        new_records = []
        for entry in feed.entries:
            record = {}
            record[primary_key] = entry.id
            record[date_field] = entry.published_parsed
            title = entry.title
            title = parseTitle(title)
            record['ADDRESS'] = title[0]
            record['ISSUE_REPORTED'] = title[1]
            new_records.append(record)

        #  convert feed fieldnames to Knack database names
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

        payload_update = datautil.reduce_to_keys(records_archive, ['id', status_key_raw])

        #  idnentify new records from current feed
        records_insert = []
        for new_rec in new_records:
            if has_match(
                kn.data,
                new_rec[primary_key_raw],
                primary_key_raw
            ):
                continue

            else:
                new_rec[status_key_raw] = 'ACTIVE'
                records_insert.append(new_rec)

        #  geocode new records
        payload_insert = []
        address_field = kn.field_map['ADDRESS']
        for new_rec in records_insert:
            geo = agolutil.geocode(geocoder, new_rec[address_field])
            new_rec = handleGeocode(geo, new_rec, kn.field_map)
            new_rec[date_field_raw] = handleDateField(new_rec[date_field_raw])
            payload_insert.append(new_rec)
        
        count = 0
        for record in payload_update:
            count += 1
            print( 'Updating record {} of {}'.format( count, len(payload_update) ) )
            
            res = knackpy.update_record(
                record,
                knack_obj,
                'id',
                knack_creds['app_id'],
                knack_creds['api_key']
            )

        count = 0
        for record in payload_insert:
            count += 1
            print( 'Inserting record {} of {}'.format( count, len(payload_insert) ) )

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



