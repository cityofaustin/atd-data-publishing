'''
Downloadt traffic study locations form ArcGIS Online
and publish to Austin's Open Data Portal
'''
import csv
import pdb
import logging
import traceback
import arrow
import secrets
import email_helpers
import socrata_helpers
import agol_helpers

socrata_creds = secrets.SOCRATA_CREDENTIALS
socrata_resource_id = 'jqhg-imb3'
agol_creds = secrets.AGOL_CREDENTIALS

fieldnames = ['COMMENT_FIELD2', 'START_DATE', 'SITE_CODE', 'COMMENT_FIELD1', 'GLOBALID', 'DATA_FILE', 'COMMENT_FIELD4', 'COMMENT_FIELD3', 'LATITUDE', 'LONGITUDE']

agol_config = {
    'service_url' : 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/Traffic_Count_Location/FeatureServer/0/',
    'query_params' : {
        'f' : 'json',
        'where' : '1=1',
        'outFields' : '*',
        'returnGeometry' : True,
         'outSr': 4326  #  return WGS84
    }   
}


def parseMills(d):
    dt = arrow.get(d/1000)
    tz = 'US/Central'
    dt = dt.replace(tzinfo=tz)
    utc = dt.to('utc').isoformat()
    return utc


def main():
    agol_config['query_params']['token'] = agol_helpers.get_token(agol_creds)
    data = agol_helpers.query_layer(agol_config['service_url'], agol_config['query_params'])

    features = []
    if data['features']:
        for feature in data['features']:
            add_feature = {a.upper():feature['attributes'][a] for a in feature['attributes'] if a.upper() in fieldnames}
            add_feature['LONGITUDE'] = float(str(feature['geometry']['x'])[:10])  #  truncate coordinate
            add_feature['LATITUDE'] = float(str(feature['geometry']['y'])[:10])
            if add_feature['START_DATE']:
                add_feature['START_DATE'] = parseMills(add_feature['START_DATE'])

            features.append(add_feature)
    
    features = socrata_helpers.create_location_fields(features)
    upsert_response = socrata_helpers.replace_data(socrata_creds, socrata_resource_id, features)
    return 'Done'


if __name__ == '__main__':

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')

    log_directory = secrets.LOG_DIRECTORY
    logfile = '{}/traffic_count_pub_{}.log'.format(log_directory, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('START TRAFFIC STUDY LOCATIONS AT {}'.format(str(now)))

    try:
        results = main()

    except Exception as e:
        error_text = traceback.format_exc()
        print(error_text)
        logging.error(error_text)
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Traffic Count Locations Process Failure', error_text)

    logging.info('END AT: {}'.format(arrow.now().format()))