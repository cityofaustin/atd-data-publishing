#  update Knack street segments with data from COA ArcGIS Online Feature Service
 
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import logging
import pdb
import arrow
import knack_helpers
import agol_helpers
import email_helpers
import data_helpers
import secrets

now = arrow.now()
now_s = now.format('YYYY_MM_DD')

#  init logging 
logfile = '{}_{}.log'.format('log/street_seg_updater', now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)

#  config
primay_key = 'SEGMENT_ID_NUMBER'
knack_creds = secrets.KNACK_CREDENTIALS
objects = ['object_7']
scene = '424'
view = '1198'


def main(date_time):
    print('starting stuff now')

    try:       

        field_dict = knack_helpers.get_fields(objects, knack_creds)

        field_lookup = knack_helpers.create_field_lookup(field_dict, parse_raw=True)
        
        field_lookup['KNACK_ID'] = 'KNACK_ID'  #  appended to field lookup to avoid dropping when keys are replaced with db fieldnames

        knack_data = knack_helpers.get_data(scene, view, knack_creds)
        
        knack_data = knack_helpers.parse_data(knack_data, field_dict, include_ids=True)

        segments_payload = []
        unmatched_segments = []

        for street_segment in knack_data:
            
            try:
                segment_data = agol_helpers.query_atx_street(street_segment[primay_key])

                if not segment_data:
                    unmatched_segments.append(street_segment[primay_key])
                    continue

                segment_data['KNACK_ID'] = street_segment['KNACK_ID']
                segment_data['MODIFIED_BY'] = 'api-update'
                segment_data['CREATED_DATE'] = arrow.now().timestamp * 1000
                segment_data['UPDATE_PROCESSED'] = True
                segments_payload.append(segment_data)
            
            except Exception as e:
                unmatched_segments.append(street_segment[primay_key])
                print("Unable to retrieve segment {}".format(street_segment[primay_key]))
                raise(e)
        
        payload = data_helpers.replace_keys(segments_payload, field_lookup, delete_unmatched=True)
        
        update_response = []
        count = 0

        for record in payload:
            count += 1
            print( 'updating record {} of {}'.format( count, len(payload) ) )

            #  remove whitespace from Esri attributes 
            for field in record:
                if type(record[field]) == str:
                    record[field] = record[field].strip()
         
            response_json = knack_helpers.update_record(record, objects[0], 'KNACK_ID', knack_creds)
            
            update_response.append(response_json)

        if (len(unmatched_segments) > 0):
            logging.info( 'Unmatched Street Segments: {}'.format(unmatched_segments) )
            email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Unmatched Street Segments', str(unmatched_segments))

        return update_response

    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        email_helpers.send_email(secrets.ALERTS_DISTRIBUTION, 'Street Segment Update Failure', str(e))
        raise e


results = main(now)

print(results)



