if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import arrow
import sys
import kits_helpers
import knack_helpers
import socrata_helpers
import data_helpers
import email_helpers
import secrets

import pdb

#  KNACK CONFIG
KNACK_PARAMS = {  
    'REFERENCE_OBJECTS' : ['object_11', 'object_12'],
    'SCENE' : '112',
    'VIEW' : '289',
    'FIELD_NAMES' : ['ATD_SIGNAL_ID','CROSS_ST','LOCATION_NAME','PRIMARY_ST', 'GEOCODE'],
    'OUT_FIELDS' : ['ATD_SIGNAL_ID','CROSS_ST','LOCATION_NAME','PRIMARY_ST', 'LATITUDE', 'LONGITUDE'],
    'APPLICATION_ID' : secrets.KNACK_CREDENTIALS['APP_ID'],
    'API_KEY' : secrets.KNACK_CREDENTIALS['API_KEY']
}

#  SOCRATA CONFIG
SOCRATA_SIGNAL_STATUS = '5zpr-dehc'
SOCRATA_SIGNAL_STATUS_HISTORICAL = 'x62n-vjpq'
SOCRATA_PUB_LOG_ID = 'n5kp-f8k4'

FLASH_STATUSES = ['1', '2', '11']

then = arrow.now()

    
def main(date_time):
    print('starting stuff now')

    try:      
        field_list = knack_helpers.GetFields(KNACK_PARAMS)

        knack_data = knack_helpers.GetData(KNACK_PARAMS)

        knack_data_parsed = knack_helpers.ParseData(knack_data, field_list, KNACK_PARAMS, require_locations=True, convert_to_unix=True)

      

        kits_query = kits_helpers.GenerateStatusIdQuery(knack_data_parsed, 'ATD_SIGNAL_ID')
        
        kits_data = kits_helpers.GetDataAsDict(secrets.KITS_CREDENTIALS, kits_query)

        kits_data = data_helpers.StringifyKeyValues(kits_data)

      

        stale = kits_helpers.CheckForStaleData(kits_data, 'OPERATION_STATE_DATETIME', 15)



        if stale['stale']:
            email_helpers.SendStaleEmail(stale['delta_minutes'], secrets.ALERTS_DISTRIBUTION)

            response_obj = { 'Errors' : 1, 'message' : 'WARNING: stale data detected' , 'Rows Updated' : 0, 'Rows Created' : 0, 'Rows Deleted' : 0 }

            stale_data_log = socrata_helpers.PrepPubLog(date_time, 'signal_status_update', response_obj)

            socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, stale_data_log, SOCRATA_PUB_LOG_ID)

            sys.exit()



        kits_data = data_helpers.FilterbyKey(kits_data, 'OPERATION_STATE', FLASH_STATUSES)  #  filter by flash statuses

        

        if kits_data:
            new_data = data_helpers.MergeDicts(knack_data_parsed, kits_data, 'ATD_SIGNAL_ID', ['OPERATION_STATE_DATETIME', 'OPERATION_STATE', 'PLAN_ID'])

        else:
            new_data = []

       

        old_data = socrata_helpers.FetchPublicData(SOCRATA_SIGNAL_STATUS)

        old_data = data_helpers.UpperCaseKeys(old_data)

        cd_results = data_helpers.DetectChanges(old_data, new_data, 'ATD_SIGNAL_ID')



        if cd_results['new'] or cd_results['change'] or cd_results['delete']:
            socrata_payload = socrata_helpers.CreatePayload(cd_results, 'ATD_SIGNAL_ID')

            socrata_payload = socrata_helpers.CreateLocationFields(socrata_payload)

            socrata_payload = data_helpers.LowerCaseKeys(socrata_payload)

            status_upsert_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, socrata_payload, SOCRATA_SIGNAL_STATUS)
        
        else:
            status_upsert_response = { 'Errors' : 0, 'message' : 'No signal status change detected' , 'Rows Updated' : 0, 'Rows Created' : 0, 'Rows Deleted' : 0 }



        log_payload = socrata_helpers.PrepPubLog(date_time, 'signal_status_update', status_upsert_response)

        pub_log_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, log_payload, SOCRATA_PUB_LOG_ID)       



        if 'error' in status_upsert_response:
            email_helpers.SendSocrataAlert(secrets.ALERTS_DISTRIBUTION, SOCRATA_SIGNAL_STATUS, status_upsert_response)
            
        elif status_upsert_response['Errors']:
            email_helpers.SendSocrataAlert(secrets.ALERTS_DISTRIBUTION, SOCRATA_SIGNAL_STATUS, status_upsert_response)



        if cd_results['delete']:

            historical_payload = data_helpers.LowerCaseKeys(cd_results['delete'])

            historical_payload = socrata_helpers.AddHistoricalFields(historical_payload)

            status_upsert_historical_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, historical_payload, SOCRATA_SIGNAL_STATUS_HISTORICAL)

            historical_log_payload = socrata_helpers.PrepPubLog(date_time, 'signal_status_historical_update', status_upsert_historical_response)

            pub_log_historical_response = socrata_helpers.UpsertData(secrets.SOCRATA_CREDENTIALS, historical_log_payload, SOCRATA_PUB_LOG_ID)



        else:
            print('no new historical status data to upload')
            status_upsert_historical_response = None
        
        return {
            'res': status_upsert_response,
            'res_historical': status_upsert_historical_response,
        }
    
    except Exception as e:
        print('Failed to process data for {}'.format(date_time))
        print(e)
        email_helpers.SendEmail(ALERTS_DISTRIBUTION, 'DATA PROCESSING ALERT: Signal Status Update Failure', str(e) + EMAIL_FOOTER)
        raise e
 

results = main(then)

print(results['res'])
print('Elapsed time: {}'.format(str(arrow.now() - then)))